{-|
Module    : Z.Data.MessagePack.Builder
Description : MessagePack builders
Copyright : (c) Hideyuki Tanaka 2009-2015
          , (c) Dong Han 2020
License   : BSD3

'Builder's to encode Haskell data types in MessagePack format.

-}

module Z.Data.MessagePack.Builder where

import           Control.Monad
import           Data.Bits
import           GHC.Int
import           Data.Word
import           Data.Primitive.PrimArray
import           GHC.Exts
import           GHC.Integer.GMP.Internals
import           Prelude                    hiding (map)
import           Z.Data.Array.Unaligned
import qualified Z.Data.Text                as T
import qualified Z.Data.Builder             as B
import qualified Z.Data.Vector              as V
import           Z.Data.MessagePack.Value   hiding (value)

value :: Value -> B.Builder ()
{-# INLINABLE value #-}
value v = case v of
    Nil      -> nil
    Bool   b -> bool b
    Int    n -> int n
    Float  f -> float f
    Double d -> double d
    Str    t -> str t
    Bin    b -> bin b
    Array  a -> array value a
    Map    m -> map value value m
    Ext  b r -> ext b r

nil :: B.Builder ()
{-# INLINE nil #-}
nil = B.word8 0xC0

bool :: Bool -> B.Builder ()
{-# INLINE bool #-}
bool False = B.word8 0xC2
bool True  = B.word8 0xC3

int :: Int64 -> B.Builder ()
{-# INLINE int #-}
int n
    | -0x20 <= n && n < 0x80         =  B.encodePrim (fromIntegral n :: Word8)
    | 0     <= n && n < 0x100        =  B.encodePrim (0xCC :: Word8, fromIntegral n :: Word8)
    | 0     <= n && n < 0x10000      =  B.encodePrim (0xCD :: Word8, BE (fromIntegral n :: Word16))
    | 0     <= n && n < 0x100000000  =  B.encodePrim (0xCE :: Word8, BE (fromIntegral n :: Word32))
    | 0     <= n                     =  B.encodePrim (0xCF :: Word8, BE (fromIntegral n :: Word64))
    | -0x80 <= n                     =  B.encodePrim (0xD0 :: Word8, fromIntegral n :: Word8)
    | -0x8000 <= n                   =  B.encodePrim (0xD1 :: Word8, BE (fromIntegral n :: Word16))
    | -0x80000000 <= n               =  B.encodePrim (0xD2 :: Word8, BE (fromIntegral n :: Word32))
    | otherwise                      =  B.encodePrim (0xD3 :: Word8, BE (fromIntegral n :: Word64))

float :: Float -> B.Builder ()
{-# INLINE float #-}
float f = B.encodePrim (0xCA :: Word8, BE f)

double :: Double -> B.Builder ()
{-# INLINE double #-}
double d = B.encodePrim (0xCB :: Word8, BE d)

-- | Construct a scientific value, see 'scientific'.
scientificValue :: Integer -> Int64 -> Value
{-# INLINE scientificValue #-}
scientificValue 0 _ = Ext 0x00 (V.pack [0x00, 0x00])
scientificValue c e = Ext (if c > 0 then 0x00 else 0x01) . B.build $ do
    int e
    B.writeN (I# (word2Int# siz#)) $ \ (MutablePrimArray mba#) (I# off#) ->
        void (exportIntegerToMutableByteArray c mba# (int2Word# off#) 1#)
  where
    siz# = sizeInBaseInteger c 256#

-- | Write a scientific value in ext 0x00(positive) and 0x01(negative) format, e.g.
--
--  +--------+--------+--------+--------+
--  |  0xD5  |  0x00  |  0x00  |  0x00  |
--  +--------+--------+--------+--------+
--
--
--  +--------+--------+--------+-----------------------------------------+---------------------------------------+
--  |  0xC7  |XXXXXXXX|  0x00  | base10 exponent(MessagePack int format) | coefficient(big endian 256-base limbs |
--  +--------+--------+--------+-----------------------------------------+---------------------------------------+
--
scientific :: Integer -> Int64 -> B.Builder ()
{-# INLINABLE scientific #-}
scientific 0 _ = B.encodePrim @(Word8, Word8, Word8, Word8) (0xD5, 0x00, 0x00, 0x00)
scientific c e = do
    case (I# (word2Int# siz#)) + intSiz e of
        1 -> B.word8 0xD4
        2 -> B.word8 0xD5
        4 -> B.word8 0xD6
        8 -> B.word8 0xD7
        16 -> B.word8 0xD8
        siz' | siz' < 0x100   -> B.encodePrim (0xC7 :: Word8, fromIntegral siz' :: Word8)
             | siz' < 0x10000 -> B.encodePrim (0xC8 :: Word8, BE (fromIntegral siz' :: Word16))
             | otherwise      -> B.encodePrim (0xC9 :: Word8, BE (fromIntegral siz' :: Word32))
    B.word8 (if c > 0 then 0x00 else 0x01)
    int e
    B.writeN (I# (word2Int# siz#)) $ \ (MutablePrimArray mba#) (I# off#) ->
        void (exportIntegerToMutableByteArray c mba# (int2Word# off#) 1#)
  where
    siz# = sizeInBaseInteger c 256#
    intSiz :: Int64 -> Int
    intSiz n
        | -0x20 <= n && n < 0x80         =  1
        | 0     <= n && n < 0x100        =  2
        | 0     <= n && n < 0x10000      =  3
        | 0     <= n && n < 0x100000000  =  5
        | 0     <= n                     =  9
        | -0x80 <= n                     =  2
        | -0x8000 <= n                   =  3
        | -0x80000000 <= n               =  5
        | otherwise                      =  9

-- | Construct a timestamp(seconds, nanoseconds) value.
timestampValue :: Int64 -> Int32 -> Value
{-# INLINE timestampValue #-}
timestampValue s ns = Ext 0xFF (B.build $ B.encodePrim (BE ns, BE s))

-- | Write a timestamp(seconds, nanoseconds) in ext 0xFF format, e.g.
timestamp :: Int64 -> Int32 -> B.Builder ()
{-# INLINE timestamp #-}
timestamp s ns = B.encodePrim
    (0xC7 :: Word8, 0x0C :: Word8, 0xFF :: Word8, (BE ns :: BE Int32), (BE s :: BE Int64))

str' :: String -> B.Builder ()
{-# INLINE str' #-}
str' = str . T.pack

str :: T.Text -> B.Builder ()
{-# INLINE str #-}
str t = do
    let bs = T.getUTF8Bytes t
    case V.length bs of
        len | len <= 31      ->  B.word8 (0xA0 .|. fromIntegral len)
            | len < 0x100    ->  B.encodePrim (0xD9 :: Word8, fromIntegral len :: Word8)
            | len < 0x10000  ->  B.encodePrim (0xDA :: Word8, BE (fromIntegral len :: Word16))
            | otherwise      ->  B.encodePrim (0xDB :: Word8, BE (fromIntegral len :: Word32))
    B.bytes bs

bin :: V.Bytes -> B.Builder ()
{-# INLINE bin #-}
bin bs = do
    case V.length bs of
        len | len < 0x100    ->  B.encodePrim (0xC4 :: Word8, fromIntegral len :: Word8)
            | len < 0x10000  ->  B.encodePrim (0xC5 :: Word8, BE (fromIntegral len :: Word16))
            | otherwise      ->  B.encodePrim (0xC6 :: Word8, BE (fromIntegral len :: Word32))
    B.bytes bs

array :: V.Vec v a => (a -> B.Builder ()) -> v a -> B.Builder ()
{-# INLINE array #-}
array p xs = do
    arrayHeader (V.length xs)
    V.traverseVec_ p xs

array' :: (a -> B.Builder ()) -> [a] -> B.Builder ()
{-# INLINE array' #-}
array' p xs = do
    arrayHeader (length xs)
    mapM_ p xs

arrayHeader :: Int -> B.Builder ()
{-# INLINE arrayHeader #-}
arrayHeader len
    | len <= 15      =  B.word8 (0x90 .|. fromIntegral len)
    | len < 0x10000  =  B.encodePrim (0xDC :: Word8, BE (fromIntegral len :: Word16))
    | otherwise      =  B.encodePrim (0xDD :: Word8, BE (fromIntegral len :: Word32))

map :: (a -> B.Builder ()) -> (b -> B.Builder ()) -> V.Vector (a, b) -> B.Builder ()
{-# INLINE map #-}
map p q xs = do
    mapHeader (V.length xs)
    V.traverseVec_ (\(a, b) -> p a >> q b) xs

map' :: (a -> B.Builder ()) -> (b -> B.Builder ()) -> [(a, b)] -> B.Builder ()
{-# INLINE map' #-}
map' p q xs = do
    mapHeader (length xs)
    mapM_ (\(a, b) -> p a >> q b) xs

mapHeader :: Int -> B.Builder ()
{-# INLINE mapHeader #-}
mapHeader len
    | len <= 15      =  B.word8 (0x80 .|. fromIntegral len)
    | len < 0x10000  =  B.encodePrim (0xDE :: Word8, BE (fromIntegral len :: Word16))
    | otherwise      =  B.encodePrim (0xDF :: Word8, BE (fromIntegral len :: Word32))

ext :: Word8 -> V.Bytes -> B.Builder ()
{-# INLINABLE ext #-}
ext typ dat = do
    case V.length dat of
        1  -> B.word8 0xD4
        2  -> B.word8 0xD5
        4  -> B.word8 0xD6
        8  -> B.word8 0xD7
        16 -> B.word8 0xD8
        len | len < 0x100   -> B.encodePrim (0xC7 :: Word8, fromIntegral len :: Word8)
            | len < 0x10000 -> B.encodePrim (0xC8 :: Word8, BE (fromIntegral len :: Word16))
            | otherwise     -> B.encodePrim (0xC9 :: Word8, BE (fromIntegral len :: Word32))
    B.word8 typ
    B.bytes dat
