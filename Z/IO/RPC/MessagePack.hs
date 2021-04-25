{-|
Module      : Z.Data.MessagePack
Description : Fast MessagePack serialization/deserialization
Copyright   : (c) Dong Han, 2019
License     : BSD
Maintainer  : winterland1989@gmail.com
Stability   : experimental
Portability : non-portable

This module provides <https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md MessagePack-RPC> implementation.
-}

module Z.IO.RPC.MessagePack
  ( -- * Example
    --
    -- $server-example
    --
    -- $client-example

    -- * Server
    ServerLoop
  , ServerService
  , ServerHandler (..)
  , SessionCtx
  , readSessionCtx
  , writeSessionCtx
  , clearSessionCtx
  , modifySessionCtx

  , serveRPC
  , serveRPC'
  , simpleRouter

    -- * Client
  , Client (..)
  , rpcClient
  , rpcClient'
  , call
  , notify

    -- ** Pipeline
  , PipelineId
  , PipelineResult
  , callPipeline
  , notifyPipeline
  , execPipeline
  , fetchPipeline

  , callStream

    -- * Misc
  , Request (..)
  , RPCException (..)
  ) where

import           Control.Concurrent
import           Control.Monad
import           Data.Bits
import           Data.IORef
import           Data.Int
import           Z.Data.MessagePack         (MessagePack)
import qualified Z.Data.MessagePack         as MP
import qualified Z.Data.MessagePack.Builder as MB
import qualified Z.Data.MessagePack.Value   as MV
import qualified Z.Data.Parser              as P
import           Z.Data.PrimRef.PrimIORef
import qualified Z.Data.Text                as T
import qualified Z.Data.Vector              as V
import qualified Z.Data.Vector.FlatIntMap   as FIM
import qualified Z.Data.Vector.FlatMap      as FM
import           Z.IO
import           Z.IO.Network

-------------------------------------------------------------------------------
-- Client

data Client = Client
    { _clientSeqRef         :: Counter
    , _clientPipelineReqNum :: Counter
    , _clientBufferedInput  :: BufferedInput
    , _clientBufferedOutput :: BufferedOutput
    }

-- | Open a RPC client from input/output device.
rpcClient :: (Input dev, Output dev) => dev -> IO Client
rpcClient uvs = rpcClient' uvs uvs V.defaultChunkSize V.defaultChunkSize

-- | Open a RPC client with more control.
rpcClient' :: (Input i, Output o)
              => i
              -> o
              -> Int          -- ^ recv buffer size
              -> Int          -- ^ send buffer size
              -> IO Client
rpcClient' i o recvBufSiz sendBufSiz = do
    seqRef <- newCounter 0
    reqNum <- newCounter 0
    bi <- newBufferedInput' recvBufSiz i
    bo <- newBufferedOutput' sendBufSiz o
    return (Client seqRef reqNum bi bo)

-- | Send a single RPC call and get result.
call:: (MessagePack req, MessagePack res, HasCallStack) => Client -> T.Text -> req -> IO res
call cli name req = do
    msgid <- callPipeline cli name req
    fetchPipeline msgid =<< execPipeline cli

-- | Send a single notification RPC call without getting result.
notify :: (MessagePack req, HasCallStack)=> Client -> T.Text -> req -> IO ()
notify c@(Client _ _ _ bo) name req = notifyPipeline c name req >> flushBuffer bo

type PipelineId = Int
type PipelineResult = FIM.FlatIntMap MV.Value

-- | Make a call inside a pipeline, which will be sent in batch when `execPipeline`.
--
-- @
--  ...
--  fooId <- callPipeline client "foo" $ ...
--  barId <- callPipeline client "bar" $ ...
--  notifyPipeline client "qux" $ ...
--
--  r <- execPipeline client
--
--  fooResult <- fetchPipeline fooId r
--  barResult <- fetchPipeline barId r
-- @
--
callPipeline :: HasCallStack => MessagePack req => Client -> T.Text -> req -> IO PipelineId
callPipeline (Client seqRef reqNum _ bo) name req = do
    x <- readPrimIORef reqNum
    when (x == (-1)) $ throwIO (RPCStreamUnconsumed callStack)
    writePrimIORef reqNum (x+1)
    msgid <- readPrimIORef seqRef
    writePrimIORef seqRef (msgid+1)
    let !msgid' = msgid .&. 0xFFFFFFFF  -- shrink to unsiged 32bits
    writeBuilder bo $ do
        MB.arrayHeader 4
        MB.int 0                        -- type request
        MB.int (fromIntegral msgid')    -- msgid
        MB.str name                     -- method name
        MP.encodeMessagePack req        -- param
    return msgid'

-- | Make a notify inside a pipeline, which will be sent in batch when `execPipeline`.
--
-- Notify calls doesn't affect execution's result.
notifyPipeline :: HasCallStack => MessagePack req => Client -> T.Text -> req -> IO ()
notifyPipeline (Client _ reqNum _ bo) name req = do
    x <- readPrimIORef reqNum
    when (x == (-1)) $ throwIO (RPCStreamUnconsumed callStack)
    writeBuilder bo $ do
        MB.arrayHeader 3
        MB.int 2                        -- type notification
        MB.str name                     -- method name
        MP.encodeMessagePack req        -- param

-- | Exception thrown when remote endpoint return errors.
data RPCException
    = RPCStreamUnconsumed CallStack
    | RPCException MV.Value CallStack
  deriving Show
instance Exception RPCException

-- | Sent request in batch and get result in a map identified by 'PipelineId'.
execPipeline :: HasCallStack => Client -> IO PipelineResult
execPipeline (Client _ reqNum bi bo) = do
    flushBuffer bo
    x <- readPrimIORef reqNum
    when (x == (-1)) $ throwIO (RPCStreamUnconsumed callStack)
    writePrimIORef reqNum 0
    FIM.packN x <$> replicateM x (do
        (msgid, err, v) <- readParser (do
            tag <- P.anyWord8
            when (tag /= 0x94) (P.fail' $ "wrong response tag: " <> T.toText tag)
            !typ <- MV.value
            !seq_ <- MV.value
            !err <- MV.value
            !v <- MV.value
            case typ of
                MV.Int 1 -> case seq_ of
                    MV.Int msgid | msgid >= 0 && msgid <= 0xFFFFFFFF ->
                        return (msgid, err, v)
                    _ -> P.fail' $ "wrong msgid: " <> T.toText seq_
                _ -> P.fail' $ "wrong response type: " <> T.toText typ
            ) bi
        when (err /= MV.Nil) $ throwIO (RPCException err callStack)
        return (V.IPair (fromIntegral msgid) v))

-- | Use the `PipelineId` returned when `callPipeline` to fetch call's result.
fetchPipeline :: HasCallStack => MessagePack res => PipelineId -> PipelineResult -> IO res
fetchPipeline msgid r = do
    unwrap "EPARSE" . MP.convertValue =<<
        unwrap' "ENOMSG" ("missing message in response: " <> T.toText msgid)
            (FIM.lookup msgid r)

-- | Call a stream method, no other `call` or `notify` should be sent until
-- returned stream is consumed completely.
--
-- This is implemented by extend MessagePack-RPC protocol by adding following new message types:
--
-- @
-- -- start stream request
-- [typ 0x04, name, param]
--
-- -- stop stream request
-- [typ 0x05]
--
-- -- each stream response
-- [typ 0x06, err, value]
--
-- -- stream response end
-- [typ 0x07]
-- @
--
-- The return tuple is a pair of a stop action and a `Source`, to terminate stream early, call the
-- stop action. Please continue consuming until EOF reached,
-- otherwise the state of the `Client` will be incorrect.
callStream :: (MessagePack req, MessagePack res, HasCallStack) => Client -> T.Text -> req -> IO (IO (), Source res)
callStream (Client _seqRef reqNum bi bo) name req = do
    x <- readPrimIORef reqNum
    when (x == (-1)) $ throwIO (RPCStreamUnconsumed callStack)
    writePrimIORef reqNum (-1)
    writeBuilder bo $ do
        MB.arrayHeader 3
        MB.int 4                        -- type request
        MB.str name                     -- method name
        MP.encodeMessagePack req        -- param
    flushBuffer bo
    return (sendEOF, sourceFromIO $ do
        res <- readParser (do
            tag <- P.anyWord8
            -- stream stop
            case tag of
                0x91 -> do
                    !typ <- MV.value
                    when (typ /= MV.Int 7) $
                        P.fail' $ "wrong response type: " <> T.toText typ
                    return Nothing
                0x93 -> do
                    !typ <- MV.value
                    !err <- MV.value
                    !v <- MV.value
                    when (typ /= MV.Int 6) $
                        P.fail' $ "wrong response type: " <> T.toText typ
                    return (Just (err, v))
                _ -> P.fail' $ "wrong response tag: " <> T.toText tag
            ) bi

        -- we take tcp disconnect as eof too
        case res of
            Just (err, v) -> do
                when (err /= MV.Nil) $ throwIO (RPCException err callStack)
                unwrap "EPARSE" (MP.convertValue v)
            _ -> do
                writePrimIORef reqNum 0
                return Nothing
        )
  where
    sendEOF = do
        writeBuilder bo $ do
            MB.arrayHeader 1
            MB.int 5
        flushBuffer bo

--------------------------------------------------------------------------------
-- Server

type ServerLoop = (UVStream -> IO ()) -> IO ()
type ServerService a = T.Text -> Maybe (ServerHandler a)

newtype SessionCtx a = SessionCtx (IORef (Maybe a))

readSessionCtx :: SessionCtx a -> IO (Maybe a)
readSessionCtx (SessionCtx x') = readIORef x'

writeSessionCtx :: SessionCtx a -> a -> IO ()
writeSessionCtx (SessionCtx x') x = writeIORef x' (Just x)

clearSessionCtx :: SessionCtx a -> IO ()
clearSessionCtx (SessionCtx x') = writeIORef x' Nothing

-- | Try to modify 'SessionCtx' if it has.
--
-- Note that you can set the modifier function to return Nothing to clear
-- SessionCtx.
modifySessionCtx :: SessionCtx a -> (a -> Maybe a) -> IO ()
modifySessionCtx (SessionCtx x') f = modifyIORef' x' (f =<<)

data ServerHandler a where
    CallHandler :: (MessagePack req, MessagePack res)
                => (SessionCtx a -> req -> IO res) -> ServerHandler a
    NotifyHandler :: MessagePack req
                  => (SessionCtx a -> req -> IO ()) -> ServerHandler a
    -- | 'StreamHandler' will receive an 'IORef' which get updated to 'True'
    -- when client send stream end packet, stream should end up ASAP.
    StreamHandler :: (MessagePack req, MessagePack res)
                  => (SessionCtx a -> IORef Bool -> req -> IO (Source res)) -> ServerHandler a

-- | Simple router using `FlatMap`, lookup name in /O(log(N))/.
--
-- @
-- import Z.IO.PRC.MessagePack
-- import Z.IO.Network
-- import Z.IO
--
-- serveRPC (startTCPServer defaultTCPServerConfig) . simpleRouter $
--  [ ("foo", CallHandler $ \\ ctx req -> do
--      ... )
--  , ("bar", CallHandler $ \\ ctx req -> do
--      ... )
--  ]
--
-- @
simpleRouter :: [(T.Text, ServerHandler a)] -> ServerService a
simpleRouter handles name = FM.lookup name handleMap
  where
    handleMap = FM.packR handles

-- | Serve a RPC service.
serveRPC :: ServerLoop -> ServerService a -> IO ()
serveRPC serve = serveRPC' serve V.defaultChunkSize V.defaultChunkSize

data Request a
    = Notify (T.Text, a)
    | Call (Int64, T.Text, a)
    | StreamStart (T.Text, a)
  deriving Show

-- | Serve a RPC service with more control.
serveRPC' :: ServerLoop
          -> Int          -- ^ recv buffer size
          -> Int          -- ^ send buffer size
          -> ServerService a -> IO ()
serveRPC' serve recvBufSiz sendBufSiz handle = serve $ \ uvs -> do
    ctx <- SessionCtx <$> newIORef Nothing
    bi <- newBufferedInput' recvBufSiz uvs
    bo <- newBufferedOutput' sendBufSiz uvs
    loop ctx bi bo
  where
    loop ctx bi bo = do
        req <- readParser sourceParser bi
        case req of
            Notify (name, v) -> do
                case handle name of
                    Just (NotifyHandler f) -> do
                        f ctx =<< unwrap "EPARSE" (MP.convertValue v)
                    _ -> throwOtherError "ENOTFOUND" "notification method not found"
                loop ctx bi bo
            Call (msgid, name, v) -> do
                case handle name of
                    Just (CallHandler f) -> do
                        res <- try (f ctx =<< unwrap "EPARSE" (MP.convertValue v))
                        writeBuilder bo $ do
                            MB.arrayHeader 4
                            MB.int 1                        -- type response
                            MB.int (fromIntegral msgid)     -- msgid
                            case res of
                                Left e -> do
                                    MB.str (T.pack $ show (e :: SomeException))
                                    MB.nil
                                Right res -> do
                                    MB.nil
                                    MP.encodeMessagePack res
                        flushBuffer bo
                    _ -> do
                        writeBuilder bo $ do
                            MB.arrayHeader 4
                            MB.int 1                        -- type response
                            MB.int (fromIntegral msgid)     -- msgid
                            MB.str $ "request method: " <> name <> " not found"
                            MB.nil
                        flushBuffer bo
                loop ctx bi bo
            StreamStart (name, v) -> do
                eofRef <- newIORef False
                -- fork new thread to get stream end notification
                forkIO $ do
                    _ <- readParser (do
                        tag <- P.anyWord8
                        -- stream stop
                        when (tag /= 0x91) $
                            P.fail' $ "wrong request tag: " <> T.toText tag
                        !typ <- MV.value
                        when (typ /= MV.Int 5) $
                            P.fail' $ "wrong request type: " <> T.toText typ
                        ) bi
                    atomicWriteIORef eofRef True

                case handle name of
                    Just (StreamHandler f) -> (do
                        src <- f ctx eofRef =<< unwrap "EPARSE" (MP.convertValue v)
                        src (writeItem bo) EOF) `catch` (\ (e :: SomeException) ->
                            writeErrorItem bo $ "error when stream: " <> T.toText e)
                    _ -> writeErrorItem bo $ "request method: " <> name <> " not found"
                loop ctx bi bo

    writeItem bo = \ mx -> do
        case mx of
            Just x -> do
                writeBuilder bo $ do
                    MB.arrayHeader 3
                    MB.int 6                        -- type stream item
                    MB.nil
                    MP.encodeMessagePack x
                flushBuffer bo
            _ -> do
                writeBuilder bo $ do
                    MB.arrayHeader 1
                    MB.int 7                        -- type stream end
                flushBuffer bo

    writeErrorItem bo msg = do
        writeBuilder bo $ do
            MB.arrayHeader 3
            MB.int 6                                -- type stream item
            MB.str msg
            MB.nil
        flushBuffer bo

-------------------------------------------------------------------------------

sourceParser :: P.Parser (Request MV.Value)
{-# INLINE sourceParser #-}
sourceParser = do
    tag <- P.anyWord8
    case tag of
        -- notify or stream start
        0x93 -> do
            !typ <- MV.value
            !name <- MV.value
            !v <- MV.value
            case typ of
                MV.Int 2 -> case name of
                    MV.Str name' -> return (Notify (name', v))
                    _ -> P.fail' $ "wrong RPC name: " <> T.toText name
                MV.Int 4 -> case name of
                    MV.Str name' -> return (StreamStart (name', v))
                    _ -> P.fail' $ "wrong RPC name: " <> T.toText name
                _ -> P.fail' $ "wrong request type: " <> T.toText typ
        -- call
        0x94 -> do
            !typ <- MV.value
            !seq_ <- MV.value
            !name <- MV.value
            !v <- MV.value
            case typ of
                MV.Int 0 -> case seq_ of
                    MV.Int msgid | msgid >= 0 && msgid <= 0xFFFFFFFF -> case name of
                        MV.Str name' -> return (Call (msgid, name', v))
                        _ -> P.fail' $ "wrong RPC name: " <> T.toText name
                    _ -> P.fail' $ "wrong msgid: " <> T.toText seq_
                _ -> P.fail' $ "wrong request type: " <> T.toText typ
        _ -> P.fail' $ "wrong request tag: " <> T.toText tag

-- $server-example
--
-- > import Data.Maybe
-- > import Z.IO.RPC.MessagePack
-- > import Z.IO.Network
-- > import Data.IORef
-- > import Z.IO
-- > import qualified Z.Data.Text as T
-- > import qualified Z.Data.Vector as V
-- >
-- > newtype ServerCtx = ServerCtx { counter :: Int }
-- >
-- > main = serveRPC (startTCPServer defaultTCPServerConfig) $ simpleRouter
-- >  [ ("hi", CallHandler $ \ctx (req :: T.Text) -> do
-- >      writeSessionCtx ctx (ServerCtx 0)
-- >      return ("hello, " <> req)
-- >    )
-- >  , ("foo", CallHandler $ \ctx (req :: Int) -> do
-- >      modifySessionCtx ctx (Just . ServerCtx . (+ 1) . counter)
-- >      return (req + 1)
-- >    )
-- >  , ("bar", CallHandler $ \ctx (req :: T.Text) -> do
-- >      counter . fromJust <$> readSessionCtx ctx
-- >    )
-- >  , ("qux", StreamHandler $ \ctx eofRef (_ :: ()) -> do
-- >     withMVar stdinBuf (\ stdin -> pure $ \ k _ -> do
--          eof <- readIORef eofRef
--          if eof
--          then k EOF
--          else do
--              r <- readBuffer stdin
--              if V.null r
--              then k EOF
--              else k (Just r))
-- >    )
-- >  ]

-- $client-example
--
-- > import Data.Maybe
-- > import Z.IO.RPC.MessagePack
-- > import Z.IO.Network
-- > import Data.IORef
-- > import Z.IO
-- > import qualified Z.Data.Text as T
-- > import qualified Z.Data.Vector as V
-- >
-- > main = withResource (initTCPClient defaultTCPClientConfig) $ \ uvs -> do
-- >   c <- rpcClient uvs
-- >   -- single call
-- >   r <- call @T.Text @T.Text c "hi" "Alice"
-- >   print r
-- >
-- >   _ <- call @Int @Int c "foo" 1
-- >   _ <- call @Int @Int c "foo" 1
-- >   x <- call @T.Text @Int c "bar" ""
-- >   print x
-- >
-- >   -- streaming result
-- >   (_, src) <- callStream c "qux" ()
-- >   runBIO $ src >|> sinkToIO (\ b -> withMVar stdoutBuf (\ bo -> do
-- >     writeBuffer bo b
-- >     flushBuffer bo))
