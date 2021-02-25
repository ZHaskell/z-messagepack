{- |
Module    : Z.Data.MessagePack.Value
Description : MessagePack object definition and parser
Copyright : (c) Hideyuki Tanaka 2009-2015
          , (c) Dong Han 2020
License   : BSD3
-}
module Z.Data.MessagePack.Value(
  -- * MessagePack Value
    Value(..)
    -- * parse into MessagePack Value
  , parseValue
  , parseValue'
  , parseValueChunks
  , parseValueChunks'
    -- * Value Parsers
  , value
  ) where

import           Control.DeepSeq
import           Control.Monad
import           Data.Bits
import           Data.Int
import           Data.Word
import           GHC.Generics               (Generic)
import           Test.QuickCheck.Arbitrary  (Arbitrary, arbitrary)
import qualified Test.QuickCheck.Gen        as Gen
import           Prelude                    hiding (map)
import qualified Z.Data.Text                as T
import qualified Z.Data.Parser              as P
import qualified Z.Data.Vector              as V

-- | Representation of MessagePack data.
data Value
    = Bool                  !Bool                   -- ^ true or false
    | Int    {-# UNPACK #-} !Int64                  -- ^ an integer
    | Float  {-# UNPACK #-} !Float                  -- ^ a floating point number
    | Double {-# UNPACK #-} !Double                 -- ^ a floating point number
    | Str    {-# UNPACK #-} !T.Text                 -- ^ a UTF-8 string
    | Bin    {-# UNPACK #-} !V.Bytes                -- ^ a byte array
    | Array  {-# UNPACK #-} !(V.Vector Value)       -- ^ a sequence of objects
    | Map    {-# UNPACK #-} !(V.Vector (Value, Value)) -- ^ key-value pairs of objects
    | Ext    {-# UNPACK #-} !Word8                  -- ^ type tag
             {-# UNPACK #-} !V.Bytes                -- ^ data payload
    | Nil                                           -- ^ nil
  deriving (Show, Eq, Ord, Generic)
  deriving anyclass T.Print

instance NFData Value where
    rnf obj = case obj of
        Array a -> rnf a
        Map   m -> rnf m
        _             -> ()

instance Arbitrary Value where
    arbitrary = Gen.sized $ \n -> Gen.oneof
        [ Bool   <$> arbitrary
        , Int    <$> negatives
        , Float  <$> arbitrary
        , Double <$> arbitrary
        , Str    <$> arbitrary
        , Bin    <$> arbitrary
        , Array  <$> Gen.resize (n `div` 2) arbitrary
        , Map    <$> Gen.resize (n `div` 4) arbitrary
        , Ext    <$> arbitrary <*> arbitrary
        , pure Nil
        ]
        where negatives = Gen.choose (minBound, -1)


value :: P.Parser Value
{-# INLINABLE value #-}
value = do
    tag <- P.anyWord8
    case tag of
        -- Nil
        0xC0 -> return Nil

        -- Bool
        0xC2 -> return (Bool False)
        0xC3 -> return (Bool True)

        -- Integer
        c | c .&. 0x80 == 0x00 -> return (Int (fromIntegral c))
          | c .&. 0xE0 == 0xE0 -> return (Int (fromIntegral (fromIntegral c :: Int8)))

        0xCC -> Int . fromIntegral <$> P.anyWord8
        0xCD -> Int . fromIntegral <$> P.decodePrimBE @Word16
        0xCE -> Int . fromIntegral <$> P.decodePrimBE @Word32
        0xCF -> Int . fromIntegral <$> P.decodePrimBE @Word64

        0xD0 -> Int . fromIntegral <$> P.decodePrim @Int8
        0xD1 -> Int . fromIntegral <$> P.decodePrimBE @Int16
        0xD2 -> Int . fromIntegral <$> P.decodePrimBE @Int32
        0xD3 -> Int . fromIntegral <$> P.decodePrimBE @Int64

        -- Float
        0xCA -> Float <$> P.decodePrimBE @Float
        -- Double
        0xCB -> Double <$> P.decodePrimBE @Double

        -- String
        t | t .&. 0xE0 == 0xA0 -> str (t .&. 0x1F)
        0xD9 -> str =<< P.anyWord8
        0xDA -> str =<< P.decodePrimBE @Word16
        0xDB -> str =<< P.decodePrimBE @Word32

        -- Binary
        0xC4 -> bin =<< P.anyWord8
        0xC5 -> bin =<< P.decodePrimBE @Word16
        0xC6 -> bin =<< P.decodePrimBE @Word32

        -- Array
        t | t .&. 0xF0 == 0x90 -> array (t .&. 0x0F)
        0xDC -> array =<< P.decodePrimBE @Word16
        0xDD -> array =<< P.decodePrimBE @Word32

        -- Map
        t | t .&. 0xF0 == 0x80 -> map (t .&. 0x0F)
        0xDE -> map =<< P.decodePrimBE @Word16
        0xDF -> map =<< P.decodePrimBE @Word32

        -- Ext
        0xD4 -> ext (1  :: Int)
        0xD5 -> ext (2  :: Int)
        0xD6 -> ext (4  :: Int)
        0xD7 -> ext (8  :: Int)
        0xD8 -> ext (16 :: Int)
        0xC7 -> ext =<< P.anyWord8
        0xC8 -> ext =<< P.decodePrimBE @Word16
        0xC9 -> ext =<< P.decodePrimBE @Word32

        -- impossible
        x -> P.fail' ("Z.Data.MessagePack: unknown tag " <> T.toText x)

  where
    str !l = do
        bs <- P.take (fromIntegral l)
        case T.validateMaybe bs of
            Just t -> return (Str t)
            _  -> P.fail' "Z.Data.MessagePack: illegal UTF8 Bytes"
    bin !l   = Bin <$> P.take (fromIntegral l)
    array !l = Array . V.packN (fromIntegral l) <$> replicateM (fromIntegral l) value
    map !l   = Map . V.packN (fromIntegral l) <$> replicateM (fromIntegral l) ((,) <$> value <*> value)
    ext !l   = Ext <$> P.decodePrim <*> P.take (fromIntegral l)

-- | Parse 'Value' without consuming trailing bytes.
parseValue :: V.Bytes -> (V.Bytes, Either P.ParseError Value)
{-# INLINE parseValue #-}
parseValue = P.parse value

-- | Parse 'Value', if there're bytes left, parsing will fail.
parseValue' :: V.Bytes -> Either P.ParseError Value
{-# INLINE parseValue' #-}
parseValue' = P.parse' (value <* P.endOfInput)

-- | Increamental parse 'Value' without consuming trailing bytes.
parseValueChunks :: Monad m => m V.Bytes -> V.Bytes -> m (V.Bytes, Either P.ParseError Value)
{-# INLINE parseValueChunks #-}
parseValueChunks = P.parseChunks value

-- | Increamental parse 'Value', if there're bytes left, parsing will fail.
parseValueChunks' :: Monad m => m V.Bytes -> V.Bytes -> m (Either P.ParseError Value)
{-# INLINE parseValueChunks' #-}
parseValueChunks' mi inp = snd <$> P.parseChunks (value <* P.endOfInput) mi inp
