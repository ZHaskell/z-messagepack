{-|
Module    : Z.Data.MessagePack.Builder
Description : MessagePack builders
Copyright : (c) Hideyuki Tanaka 2009-2015
          , (c) Dong Han 2020
License   : BSD3

'Builder's to encode in MessagePack format.

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
    | -0x20 <= n && n < 0x80         =  B.word8 (fromIntegral n)
    | 0     <= n && n < 0x100        =  B.word8 0xCC >> B.word8 (fromIntegral n)
    | 0     <= n && n < 0x10000      =  B.word8 0xCD >> B.encodePrimBE @Word16 (fromIntegral n)
    | 0     <= n && n < 0x100000000  =  B.word8 0xCE >> B.encodePrimBE @Word32 (fromIntegral n)
    | 0     <= n                     =  B.word8 0xCF >> B.encodePrimBE @Word64 (fromIntegral n)
    | -0x80 <= n                     =  B.word8 0xD0 >> B.word8 (fromIntegral n)
    | -0x8000 <= n                   =  B.word8 0xD1 >> B.encodePrimBE @Word16 (fromIntegral n)
    | -0x80000000 <= n               =  B.word8 0xD2 >> B.encodePrimBE @Word32 (fromIntegral n)
    | otherwise                      =  B.word8 0xD3 >> B.encodePrimBE @Word64 (fromIntegral n)

float :: Float -> B.Builder ()
{-# INLINE float #-}
float f = B.word8 0xCA >> B.encodePrimBE f

double :: Double -> B.Builder ()
{-# INLINE double #-}
double d = B.word8 0xCB >> B.encodePrimBE d


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
{-# INLINE scientific #-}
scientific 0 _ = B.encodePrim @(Word8, Word8, Word8, Word8) (0xD5, 0x00, 0x00, 0x00)
scientific c e = do
    case (I# (word2Int# siz#)) + intSiz e of
        1 -> B.word8 0xD4
        2 -> B.word8 0xD5
        4 -> B.word8 0xD6
        8 -> B.word8 0xD7
        16 -> B.word8 0xD8
        siz' | siz' < 0x100   -> B.word8 0xC7 >> B.word8 (fromIntegral siz')
             | siz' < 0x10000 -> B.word8 0xC8 >> B.encodePrimBE @Word16 (fromIntegral siz')
             | otherwise      -> B.word8 0xC9 >> B.encodePrimBE @Word32 (fromIntegral siz')
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
timestampValue s ns = Ext 0xFF (B.build $ B.encodePrimBE ns >> B.encodePrimBE s)

-- | Write a timestamp(seconds, nanoseconds) in ext 0xFF format, e.g.
timestamp :: Int64 -> Int32 -> B.Builder ()
{-# INLINE timestamp #-}
timestamp s ns = B.encodePrim @(Word8, Word8, Word8, (BE Int32), (BE Int64)) (0xC7, 0x0C, 0xFF, (BE ns), (BE s))

str' :: String -> B.Builder ()
{-# INLINE str' #-}
str' = str . T.pack

str :: T.Text -> B.Builder ()
{-# INLINE str #-}
str t = do
    let bs = T.getUTF8Bytes t
    case V.length bs of
        len | len <= 31      ->  B.word8 (0xA0 .|. fromIntegral len)
            | len < 0x100    ->  B.word8 0xD9 >> B.word8 (fromIntegral len)
            | len < 0x10000  ->  B.word8 0xDA >> B.encodePrimBE @Word16 (fromIntegral len)
            | otherwise      ->  B.word8 0xDB >> B.encodePrimBE @Word32 (fromIntegral len)
    B.bytes bs

bin :: V.Bytes -> B.Builder ()
{-# INLINE bin #-}
bin bs = do
    case V.length bs of
        len | len < 0x100    ->  B.word8 0xC4 >> B.word8 (fromIntegral len)
            | len < 0x10000  ->  B.word8 0xC5 >> B.encodePrimBE @Word16 (fromIntegral len)
            | otherwise      ->  B.word8 0xC6 >> B.encodePrimBE @Word32 (fromIntegral len)
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
    | len < 0x10000  =  B.word8 0xDC >> B.encodePrimBE @Word16 (fromIntegral len)
    | otherwise      =  B.word8 0xDD >> B.encodePrimBE @Word32 (fromIntegral len)

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
    | len < 0x10000  =  B.word8 0xDE >> B.encodePrimBE @Word16 (fromIntegral len)
    | otherwise      =  B.word8 0xDF >> B.encodePrimBE @Word32 (fromIntegral len)

ext :: Word8 -> V.Bytes -> B.Builder ()
{-# INLINABLE ext #-}
ext typ dat = do
    case V.length dat of
        1  -> B.word8 0xD4
        2  -> B.word8 0xD5
        4  -> B.word8 0xD6
        8  -> B.word8 0xD7
        16 -> B.word8 0xD8
        len | len < 0x100   -> B.word8 0xC7 >> B.word8 (fromIntegral len)
            | len < 0x10000 -> B.word8 0xC8 >> B.encodePrimBE @Word16 (fromIntegral len)
            | otherwise     -> B.word8 0xC9 >> B.encodePrimBE @Word32 (fromIntegral len)
    B.word8 typ
    B.bytes dat
