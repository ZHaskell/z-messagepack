{-|
Module      : Z.Data.MessagePack
Description : Fast MessagePack serialization/deserialization
Copyright   : (c) Dong Han, 2019
License     : BSD
Maintainer  : winterland1989@gmail.com
Stability   : experimental
Portability : non-portable

This module provides <https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md MessagePack-RPC> implementation.

@
-- server
import Z.IO.RPC.MessagePack
import Z.IO.Network
import Z.IO
import qualified Z.Data.Text as T

serveRPC (startTCPServer defaultTCPServerConfig) . simpleRouter $
 [ ("foo", CallHandler $ \\ (req :: Int) -> do
     return (req + 1))
 , ("bar", NotifyHandler $ \\ (req :: T.Text) -> do
     printStd (req <> "world"))
 , ("qux", StreamHandler $ \\ (_ :: ()) -> do
    withMVar stdinBuf (pure . sourceFromBuffered))
 ]

-- client
import Z.IO.RPC.MessagePack
import Z.IO.Network
import Z.IO
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V

withResource (initTCPClient defaultTCPClientConfig) $ \\ uvs -> do
    c <- rpcClient uvs
    -- single call
    call \@Int \@Int c "foo" 1
    -- notify without result
    notify \@T.Text c "bar" "hello"
    -- streaming result
    (_, src) <- callStream c "qux" ()
    runBIO $ src >|> sinkToIO (\\ b -> withMVar stdoutBuf (\\ bo -> do
        writeBuffer bo b
        flushBuffer bo))

@

-}

module Z.IO.RPC.MessagePack where

import           Control.Concurrent
import           Control.Monad
import           Data.Bits
import           Data.Int
import           Data.IORef
import           Z.Data.PrimRef.PrimIORef
import qualified Z.Data.MessagePack.Builder as MB
import qualified Z.Data.MessagePack.Value   as MV
import           Z.Data.MessagePack         (MessagePack)
import qualified Z.Data.MessagePack         as MP
import qualified Z.Data.Parser              as P
import qualified Z.Data.Text                as T
import qualified Z.Data.Vector.FlatIntMap   as FIM
import qualified Z.Data.Vector.FlatMap      as FM
import qualified Z.Data.Vector              as V
import           Z.IO
import           Z.IO.Network

data Client = Client
    { _clientSeqRef :: Counter
    , _clientPipelineReqNum :: Counter
    , _clientBufferedInput :: BufferedInput
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
            !seq <- MV.value
            !err <- MV.value
            !v <- MV.value
            case typ of
                MV.Int 1 -> case seq of
                    MV.Int msgid | msgid >= 0 && msgid <= 0xFFFFFFFF ->
                        return (msgid, err, v)
                    _ -> P.fail' $ "wrong msgid: " <> T.toText seq
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
callStream (Client seqRef reqNum bi bo) name req = do
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
        res <- pull (sourceParserFromBuffered (do
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
            ) bi)

        -- we take tcp disconnect as eof too
        case (join res) of
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

type ServerLoop = (UVStream -> IO ()) -> IO ()
type ServerService = T.Text -> Maybe ServerHandler
data ServerHandler where
    CallHandler :: (MessagePack req, MessagePack res) => (req -> IO res) -> ServerHandler
    NotifyHandler :: MessagePack req => (req -> IO ()) -> ServerHandler
    StreamHandler :: (MessagePack req, MessagePack res) => (req -> IO (Source res)) -> ServerHandler

-- | Simple router using `FlatMap`, lookup name in /O(log(N))/.
--
-- @
-- import Z.IO.PRC.MessagePack
-- import Z.IO.Network
-- import Z.IO
--
-- serveRPC (startTCPServer defaultTCPServerConfig) . simpleRouter $
--  [ ("foo", CallHandler $ \\ req -> do
--      ... )
--  , ("bar", CallHandler $ \\ req -> do
--      ... )
--  ]
--
-- @
simpleRouter :: [(T.Text, ServerHandler)] -> ServerService
simpleRouter handles name = FM.lookup name handleMap
  where
    handleMap = FM.packR handles

-- | Serve a RPC service.
serveRPC :: ServerLoop -> ServerService -> IO ()
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
          -> ServerService -> IO ()
serveRPC' serve recvBufSiz sendBufSiz handle = serve $ \ uvs -> do
    bi <- newBufferedInput' recvBufSiz uvs
    bo <- newBufferedOutput' sendBufSiz uvs
    loop bi bo
  where
    loop bi bo = do
        req <- pull (sourceParserFromBuffered (do
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
                    !seq <- MV.value
                    !name <- MV.value
                    !v <- MV.value
                    case typ of
                        MV.Int 0 -> case seq of
                            MV.Int msgid | msgid >= 0 && msgid <= 0xFFFFFFFF -> case name of
                                MV.Str name' -> return (Call (msgid, name', v))
                                _ -> P.fail' $ "wrong RPC name: " <> T.toText name
                            _ -> P.fail' $ "wrong msgid: " <> T.toText seq
                        _ -> P.fail' $ "wrong request type: " <> T.toText typ
                _ -> P.fail' $ "wrong request tag: " <> T.toText tag
            ) bi)
        print req
        case req of
            Just (Notify (name, v)) -> do
                case handle name of
                    Just (NotifyHandler f) -> do
                        f =<< unwrap "EPARSE" (MP.convertValue v)
                    _ -> throwOtherError "ENOTFOUND" "notification method not found"
                loop bi bo
            Just (Call (msgid, name, v)) -> do
                case handle name of
                    Just (CallHandler f) -> do
                        res <- try (f =<< unwrap "EPARSE" (MP.convertValue v))
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
                loop bi bo
            Just (StreamStart (name, v)) -> do
                eofRef <- newIORef False
                -- fork new thread to get stream end notification
                forkIO $ do
                    pull (sourceParserFromBuffered (do
                        tag <- P.anyWord8
                        -- stream stop
                        when (tag /= 0x91) $
                            P.fail' $ "wrong request tag: " <> T.toText tag
                        !typ <- MV.value
                        when (typ /= MV.Int 5) $
                            P.fail' $ "wrong request type: " <> T.toText typ
                        ) bi)
                    atomicWriteIORef eofRef True

                case handle name of
                    Just (StreamHandler f) -> do
                        src <- f =<< unwrap "EPARSE" (MP.convertValue v)
                        loopSend eofRef src bo
                    _ -> do
                        writeBuilder bo $ do
                            MB.arrayHeader 3
                            MB.int 6                        -- type response
                            MB.str $ "request method: " <> name <> " not found"
                            MB.nil
                        flushBuffer bo
                loop bi bo

            _ -> return ()

    loopSend eofRef src bo = do
        eof <- readIORef eofRef
        if eof
        then do
            writeBuilder bo $ do
                MB.arrayHeader 1
                MB.int 7                        -- type response
            flushBuffer bo
        else do
            r <- pull src
            case r of
                Just r' -> do
                    writeBuilder bo $ do
                        MB.arrayHeader 3
                        MB.int 6                        -- type response
                        MB.nil
                        MP.encodeMessagePack r'
                    flushBuffer bo
                _ -> atomicWriteIORef eofRef True
            loopSend eofRef src bo
