{-|
Module      : Z.MessagePack.Base
Description : Fast MessagePack serialization/deserialization
Copyright   : (c) Dong Han, 2020
License     : BSD
Maintainer  : winterland1989@gmail.com
Stability   : experimental
Portability : non-portable

This module provides 'Converter' to convert 'Value' to haskell data types, and various tools to help user define 'MessagePack' instance.

-}

module Z.MessagePack.Base
  -- * MessagePack Class
  ( MessagePack(..), Value(..), defaultSettings, Settings(..)
  , -- * Encode & Decode
    DecodeError
  , decode, decode', decodeChunks, decodeChunks', encode, encodeChunks
    -- * parse into MessagePack Value
  , MV.parseValue, MV.parseValue', MV.parseValueChunks, MV.parseValueChunks'
  -- * Generic FromValue, ToValue & EncodeMessagePack
  , gToValue, gFromValue, gEncodeMessagePack
  -- * Convert 'Value' to Haskell data
  , convertValue, Converter(..), fail', (<?>), prependContext
  , PathElement(..), ConvertError(..)
  , typeMismatch, fromNil, withBool
  , withStr, withBin, withArray, withKeyValues, withFlatMap, withFlatMapR
  , (.:), (.:?), (.:!), convertField, convertFieldMaybe, convertFieldMaybe'
  -- * Helper for manually writing instance.
  , (.=), object, (.!), object', KVItem
  ) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.ST
import           Data.Char                      (ord)
import           Data.Data
import           Data.Fixed
import           Data.Functor.Compose
import           Data.Functor.Const
import           Data.Functor.Identity
import           Data.Functor.Product
import           Data.Functor.Sum
import qualified Data.Foldable                  as Foldable
import           Data.Hashable
import qualified Data.HashMap.Strict            as HM
import qualified Data.HashSet                   as HS
import qualified Data.IntMap                    as IM
import qualified Data.IntSet                    as IS
import qualified Data.Map.Strict                as M
import qualified Data.Sequence                  as Seq
import qualified Data.Set                       as Set
import qualified Data.Tree                      as Tree
import           GHC.Int
import           GHC.Exts
import           Data.List.NonEmpty             (NonEmpty (..))
import qualified Data.List.NonEmpty             as NonEmpty
import qualified Data.Monoid                    as Monoid
import qualified Data.Primitive.ByteArray       as A
import qualified Data.Primitive.SmallArray      as A
import           Data.Primitive.Types           (Prim)
import           Data.Proxy                     (Proxy (..))
import           Data.Ratio                     (Ratio, denominator, numerator, (%))
import           Data.Scientific                (Scientific, coefficient, base10Exponent)
import qualified Data.Scientific                as Sci
import qualified Data.Semigroup                 as Semigroup
import           Data.Tagged                    (Tagged (..))
import           Data.Time                      (Day, DiffTime, LocalTime, NominalDiffTime, TimeOfDay, UTCTime, ZonedTime)
import           Data.Time.Calendar             (CalendarDiffDays (..), DayOfWeek (..))
import           Data.Time.LocalTime            (CalendarDiffTime (..))
import           Data.Time.Clock.System         (SystemTime (..), systemToUTCTime, utcToSystemTime)
import           Data.Version                   (Version, parseVersion)
import           Data.Word
import           Foreign.C.Types
import           GHC.Exts                       (Proxy#, proxy#)
import           GHC.Generics
import           GHC.Natural
import           GHC.Integer.GMP.Internals
import           System.Exit
import           Text.ParserCombinators.ReadP   (readP_to_S)
import qualified Z.Data.Array                   as A
import qualified Z.Data.Builder                 as B
import           Z.Data.Generics.Utils
import qualified Z.MessagePack.Builder          as MB
import           Z.Data.JSON.Converter
import           Z.MessagePack.Value            (Value (..))
import qualified Z.MessagePack.Value            as MV
import qualified Z.Data.Parser                  as P
import qualified Z.Data.Parser.Numeric          as P
import qualified Z.Data.Text.Base               as T
import qualified Z.Data.Text                    as T
import qualified Z.Data.Text.Print              as T
import qualified Z.Data.Vector.Base             as V
import qualified Z.Data.Vector.Extra            as V
import qualified Z.Data.Vector.FlatIntMap       as FIM
import qualified Z.Data.Vector.FlatIntSet       as FIS
import qualified Z.Data.Vector.FlatMap          as FM
import qualified Z.Data.Vector.FlatSet          as FS

--------------------------------------------------------------------------------

-- | Type class for encode & decode MessagePack.
class MessagePack a where
    fromValue :: Value -> Converter a
    default fromValue :: (Generic a, GFromValue (Rep a)) => Value -> Converter a
    fromValue v = to <$> gFromValue defaultSettings v
    {-# INLINABLE fromValue #-}

    toValue :: a -> Value
    default toValue :: (Generic a, GToValue (Rep a)) => a -> Value
    toValue = gToValue defaultSettings . from
    {-# INLINABLE toValue #-}

    encodeMessagePack :: a -> B.Builder ()
    default encodeMessagePack :: (Generic a, GEncodeMessagePack (Rep a)) => a -> B.Builder ()
    encodeMessagePack = gEncodeMessagePack defaultSettings . from
    {-# INLINABLE encodeMessagePack #-}

--------------------------------------------------------------------------------

-- There're two possible failures here:
--
--   * 'P.ParseError' is an error during parsing bytes to 'Value'.
--   * 'ConvertError' is an error when converting 'Value' to target data type.
type DecodeError = Either P.ParseError ConvertError

-- | Decode a MessagePack doc, trailing bytes are not allowed.
decode' :: MessagePack a => V.Bytes -> Either DecodeError a
{-# INLINE decode' #-}
decode' bs = case P.parse' (MV.value <* P.endOfInput) bs of
    Left pErr -> Left (Left pErr)
    Right v -> case convertValue v of
        Left cErr -> Left (Right cErr)
        Right r   -> Right r

-- | Decode a MessagePack bytes, return any trailing bytes.
decode :: MessagePack a => V.Bytes -> (V.Bytes, Either DecodeError a)
{-# INLINE decode #-}
decode bs = case P.parse MV.value bs of
    (bs', Left pErr) -> (bs', Left (Left pErr))
    (bs', Right v) -> case convertValue v of
        Left cErr -> (bs', Left (Right cErr))
        Right r   -> (bs', Right r)

-- | Decode MessagePack doc chunks, return trailing bytes.
decodeChunks :: (MessagePack a, Monad m) => m V.Bytes -> V.Bytes -> m (V.Bytes, Either DecodeError a)
{-# INLINE decodeChunks #-}
decodeChunks mb bs = do
    mr <- P.parseChunks MV.value mb bs
    case mr of
        (bs', Left pErr) -> pure (bs', Left (Left pErr))
        (bs', Right v) ->
            case convertValue v of
                Left cErr -> pure (bs', Left (Right cErr))
                Right r   -> pure (bs', Right r)

-- | Decode MessagePack doc chunks, trailing bytes are not allowed.
decodeChunks' :: (MessagePack a, Monad m) => m V.Bytes -> V.Bytes -> m (Either DecodeError a)
{-# INLINE decodeChunks' #-}
decodeChunks' mb bs = do
    mr <- P.parseChunks (MV.value <* P.endOfInput) mb bs
    case mr of
        (_, Left pErr) -> pure (Left (Left pErr))
        (_, Right v) ->
            case convertValue v of
                Left cErr -> pure (Left (Right cErr))
                Right r   -> pure (Right r)

-- | Directly encode data to MessagePack bytes.
encode :: MessagePack a => a -> V.Bytes
{-# INLINE encode #-}
encode = B.build . encodeMessagePack

-- | Encode data to MessagePack bytes chunks.
encodeChunks :: MessagePack a => a -> [V.Bytes]
{-# INLINE encodeChunks #-}
encodeChunks = B.buildChunks . encodeMessagePack

-- | Run a 'Converter' with input value.
convertValue :: (MessagePack a) => Value -> Either ConvertError a
{-# INLINE convertValue #-}
convertValue = convert fromValue

--------------------------------------------------------------------------------

-- | Produce an error message like @converting XXX failed, expected XXX, encountered XXX@.
typeMismatch :: T.Text     -- ^ The name of the type you are trying to convert.
             -> T.Text     -- ^ The MessagePack value type you expecting to meet.
             -> Value      -- ^ The actual value encountered.
             -> Converter a
{-# INLINE typeMismatch #-}
typeMismatch name expected v =
    fail' $ T.concat ["converting ", name, " failed, expected ", expected, ", encountered ", actual]
  where
    actual = case v of
        Nil      ->  "Nil"
        Bool _   ->  "Bool"
        Int  _   ->  "Int"
        Float _  ->  "Float"
        Double _ ->  "Double"
        Str _    ->  "Str"
        Bin _    ->  "Bin"
        Array _  ->  "Array"
        Map _    ->  "Map"
        Ext _ _  ->  "Ext"

fromNil :: T.Text -> a -> Value -> Converter a
{-# INLINE fromNil #-}
fromNil _ a Nil = pure a
fromNil c _ v    = typeMismatch c "Nil" v

withBool :: T.Text -> (Bool -> Converter a) -> Value ->  Converter a
{-# INLINE withBool #-}
withBool _    f (Bool x) = f x
withBool name _ v        = typeMismatch name "Bool" v

withStr :: T.Text -> (T.Text -> Converter a) -> Value -> Converter a
{-# INLINE withStr #-}
withStr _    f (Str x) = f x
withStr name _ v       = typeMismatch name "Str" v

withBin :: T.Text -> (V.Bytes -> Converter a) -> Value -> Converter a
{-# INLINE withBin #-}
withBin _    f (Bin x) = f x
withBin name _ v       = typeMismatch name "Bin" v

-- | @'withBoundedScientific' name f value@ applies @f@ to the 'Scientific' number
-- when @value@ is a 'Ext' @0x00@ with exponent less than or equal to 1024.
withBoundedScientific :: T.Text -> (Scientific -> Converter a) -> Value ->  Converter a
{-# INLINE withBoundedScientific #-}
withBoundedScientific name f v = withScientific name f' v
  where
    f' x | e <= 1024 = f x
         | otherwise = fail' . B.unsafeBuildText $ do
            "converting "
            T.text name
            " failed, found a number with exponent "
            T.int e
            ", but it must not be greater than 1024"
      where e = base10Exponent x

-- | @'withScientific' name f value@ applies @f@ to the 'Scientific' number
-- when @value@ is a 'Ext' @0x00@, fails using 'typeMismatch' otherwise.
--
-- /Warning/: If you are converting from a scientific to an unbounded
-- type such as 'Integer' you may want to add a restriction on the
-- size of the exponent (see 'withBoundedScientific') to prevent
-- malicious input from filling up the memory of the target system.
--
-- ==== Error message example
--
-- > withScientific "MyType" f (Str "oops")
-- > -- Error: "converting MyType failed, expected Ext 0x00, but encountered Str"
withScientific :: T.Text -> (Scientific -> Converter a) -> Value ->  Converter a
{-# INLINE withScientific #-}
withScientific name f (Ext tag x) | tag <= 0x01 = do
    case P.parse MV.value x of
        (rest, Right (Int d)) ->  mkSci (fromIntegral d) rest
        (_, Right v) -> typeMismatch (name <> "(exponent)") "Int" v
        (_, Left e) -> fail' (T.concat ["converting ", name, " failed: ", T.toText e])
  where
    mkSci !e (V.PrimVector (A.PrimArray ba#) (I# s#) (I# l#)) =
        let !c = importIntegerFromByteArray ba# (int2Word# s#) (int2Word# l#) 1#
        in if tag == 0x01 then f (negate (Sci.scientific c e))
                          else f (Sci.scientific c e)
withScientific name _ v = typeMismatch name "Ext 0x00" v

withSystemTime :: T.Text -> (SystemTime -> Converter a) -> Value ->  Converter a
{-# INLINE withSystemTime #-}
withSystemTime name f (Ext tag x) | tag == 0xFF = do
    case P.parse' (do
        !ns <- P.decodePrimBE @Word32
        !s <- P.decodePrimBE
        pure (MkSystemTime s (fromIntegral ns))) x of
            Left e -> fail' ("parse Ext 0xFF timestamp format failed: " <> T.toText e)
            Right v -> f v
withSystemTime name _ v = typeMismatch name "Ext 0x00" v

withArray :: T.Text -> (V.Vector Value -> Converter a) -> Value -> Converter a
{-# INLINE withArray #-}
withArray _ f (Array arr) = f arr
withArray name _ v      = typeMismatch name "Arr" v

-- | Directly use 'Map' as key-values for further converting.
withKeyValues :: T.Text -> (V.Vector (Value, Value) -> Converter a) -> Value -> Converter a
{-# INLINE withKeyValues #-}
withKeyValues _    f (Map kvs) = f kvs
withKeyValues name _ v            = typeMismatch name "Map" v

-- | Take a 'Map' as an 'FM.FlatMap Value Value', on key duplication prefer first one.
withFlatMap :: T.Text -> (FM.FlatMap Value Value -> Converter a) -> Value -> Converter a
{-# INLINE withFlatMap #-}
withFlatMap _    f (Map obj) = f (FM.packVector obj)
withFlatMap name _ v            = typeMismatch name "Map" v

-- | Take a 'Map' as an 'FM.FlatMap Value Value', on key duplication prefer last one.
withFlatMapR :: T.Text -> (FM.FlatMap Value Value -> Converter a) -> Value -> Converter a
{-# INLINE withFlatMapR #-}
withFlatMapR _    f (Map obj) = f (FM.packVectorR obj)
withFlatMapR name _ v            = typeMismatch name "Map" v

-- | Retrieve the value associated with the given key of an 'Map'.
-- The result is 'empty' if the key is not present or the value cannot
-- be converted to the desired type.
--
-- This accessor is appropriate if the key and value /must/ be present
-- in an object for it to be valid.  If the key and value are
-- optional, use '.:?' instead.
(.:) :: (MessagePack a) => FM.FlatMap Value Value -> T.Text -> Converter a
{-# INLINE (.:) #-}
(.:) = convertField fromValue

-- | Retrieve the value associated with the given key of an 'Map'. The
-- result is 'Nothing' if the key is not present or if its value is 'Nil',
-- or fail if the value cannot be converted to the desired type.
--
-- This accessor is most useful if the key and value can be absent
-- from an object without affecting its validity.  If the key and
-- value are mandatory, use '.:' instead.
(.:?) :: (MessagePack a) => FM.FlatMap Value Value -> T.Text -> Converter (Maybe a)
{-# INLINE (.:?) #-}
(.:?) = convertFieldMaybe fromValue

-- | Retrieve the value associated with the given key of an 'Map'.
-- The result is 'Nothing' if the key is not present or fail if the
-- value cannot be converted to the desired type.
--
-- This differs from '.:?' by attempting to convert 'Nil' the same as any
-- other MessagePack value, instead of interpreting it as 'Nothing'.
(.:!) :: (MessagePack a) => FM.FlatMap Value Value -> T.Text -> Converter (Maybe a)
{-# INLINE (.:!) #-}
(.:!) = convertFieldMaybe' fromValue

convertField :: (Value -> Converter a)  -- ^ the field converter (value part of a key value pair)
           -> FM.FlatMap Value Value -> T.Text -> Converter a
{-# INLINE convertField #-}
convertField p obj key = case FM.lookup (Str key) obj of
    Just v -> p v <?> Key key
    _      -> fail' (T.concat $ ["key ", key, " not present"])

-- | Variant of '.:?' with explicit converter function.
convertFieldMaybe :: (Value -> Converter a) -> FM.FlatMap Value Value -> T.Text -> Converter (Maybe a)
{-# INLINE convertFieldMaybe #-}
convertFieldMaybe p obj key = case FM.lookup (Str key) obj of
    Just Nil -> pure Nothing
    Just v    -> Just <$> p v <?> Key key
    _         -> pure Nothing

-- | Variant of '.:!' with explicit converter function.
convertFieldMaybe' :: (Value -> Converter a) -> FM.FlatMap Value Value -> T.Text -> Converter (Maybe a)
{-# INLINE convertFieldMaybe' #-}
convertFieldMaybe' p obj key = case FM.lookup (Str key) obj of
    Just v -> Just <$> p v <?> Key key
    _      -> pure Nothing

--------------------------------------------------------------------------------

-- | A newtype for 'B.Builder', whose semigroup's instance is to connect kv builder and sum kv length.
data KVItem = KVItem {-# UNPACK #-} !Int (B.Builder ())

instance Semigroup KVItem where
    {-# INLINE (<>) #-}
    KVItem siza a <> KVItem sizb b = KVItem (siza+sizb) (a >> b)

-- | Connect key and value to a 'KVItem' using 'B.colon', key will be escaped.
(.!) :: MessagePack v => T.Text -> v -> KVItem
{-# INLINE (.!) #-}
k .! v = KVItem 1 (MB.str k >> encodeMessagePack v)
infixr 8 .!

-- | Write map header and 'KVItem's.
object' :: KVItem -> B.Builder ()
{-# INLINE object' #-}
object' (KVItem siz kvb) = MB.mapHeader siz >> kvb

-- | Connect key and value to a tuple to be used with 'object'.
(.=) :: MessagePack v => T.Text -> v -> (Value, Value)
{-# INLINE (.=) #-}
k .= v = (Str k, toValue v)
infixr 8 .=

-- | Alias for @Map . pack@.
object :: [(Value, Value)] -> Value
{-# INLINE object #-}
object = Map . V.pack

--------------------------------------------------------------------------------
-- | Generic encode/decode Settings
--
data Settings = Settings
    { fieldFmt  :: String -> T.Text -- ^ format field labels
    , constrFmt :: String -> T.Text -- ^ format constructor names
    , missingKeyAsNil :: Bool      -- ^ take missing field as 'Nil'?
    }

-- | @Settings T.pack T.pack False@
defaultSettings :: Settings
defaultSettings = Settings T.pack T.pack False

--------------------------------------------------------------------------------
-- GToValue
--------------------------------------------------------------------------------

class GToValue f where
    gToValue :: Settings -> f a -> Value

--------------------------------------------------------------------------------
-- Selectors

type family Field f where
    Field (a :*: b) = Field a
    Field (S1 (MetaSel Nothing u ss ds) f) = Value
    Field (S1 (MetaSel (Just l) u ss ds) f) = (Value, Value)

class GWriteFields f where
    gWriteFields :: Settings -> A.SmallMutableArray s (Field f) -> Int -> f a -> ST s ()

instance (ProductSize a, GWriteFields a, GWriteFields b, Field a ~ Field b) => GWriteFields (a :*: b) where
    {-# INLINE gWriteFields #-}
    gWriteFields s marr idx (a :*: b) = do
        gWriteFields s marr idx a
        gWriteFields s marr (idx + productSize (proxy# :: Proxy# a)) b

instance (GToValue f) => GWriteFields (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gWriteFields #-}
    gWriteFields s marr idx (M1 x) = A.writeSmallArray marr idx (gToValue s x)

instance (GToValue f, Selector (MetaSel (Just l) u ss ds)) => GWriteFields (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gWriteFields #-}
    gWriteFields s marr idx m1@(M1 x) = A.writeSmallArray marr idx ((Str $ (fieldFmt s) (selName m1)), gToValue s x)

instance (GToValue f, Selector (MetaSel (Just l) u ss ds)) => GToValue (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gToValue #-}
    gToValue s m1@(M1 x) =
        let k = fieldFmt s $ selName m1
            v = gToValue s x
        in Map (V.singleton (Str k, v))

instance GToValue f => GToValue (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gToValue #-}
    gToValue s (M1 x) = gToValue s x

instance MessagePack a => GToValue (K1 i a) where
    {-# INLINE gToValue #-}
    gToValue _ (K1 x) = toValue x

class GMergeFields f where
    gMergeFields :: Proxy# f -> A.SmallMutableArray s (Field f) -> ST s Value

instance GMergeFields a => GMergeFields (a :*: b) where
    {-# INLINE gMergeFields #-}
    gMergeFields _ = gMergeFields (proxy# :: Proxy# a)

instance GMergeFields (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gMergeFields #-}
    gMergeFields _ marr = do
        arr <- A.unsafeFreezeSmallArray marr
        let l = A.sizeofSmallArray arr
        pure (Array (V.Vector arr 0 l))

instance GMergeFields (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gMergeFields #-}
    gMergeFields _ marr = do
        arr <- A.unsafeFreezeSmallArray marr
        let l = A.sizeofSmallArray arr
        pure (Map (V.Vector arr 0 l))

--------------------------------------------------------------------------------
-- Constructors

class GConstrToValue f where
    gConstrToValue :: Bool -> Settings -> f a -> Value

instance GConstrToValue V1 where
    {-# INLINE gConstrToValue #-}
    gConstrToValue _ _ _ = error "Z.Data.MessagePack.Base: empty data type"

instance (GConstrToValue f, GConstrToValue g) => GConstrToValue (f :+: g) where
    {-# INLINE gConstrToValue #-}
    gConstrToValue _ s (L1 x) = gConstrToValue True s x
    gConstrToValue _ s (R1 x) = gConstrToValue True s x

-- | Constructor without payload, convert to String
instance (Constructor c) => GConstrToValue (C1 c U1) where
    {-# INLINE gConstrToValue #-}
    gConstrToValue _ s (M1 _) = Str . constrFmt s $ conName (undefined :: t c U1 a)

-- | Constructor with a single payload
instance (Constructor c, GToValue (S1 sc f)) => GConstrToValue (C1 c (S1 sc f)) where
    {-# INLINE gConstrToValue #-}
    gConstrToValue False s (M1 x) = gToValue s x
    gConstrToValue True s (M1 x) =
        let !k = constrFmt s $ conName @c undefined
            !v = gToValue s x
        in Map (V.singleton (Str k, v))

-- | Constructor with multiple payloads
instance (ProductSize (a :*: b), GWriteFields (a :*: b), GMergeFields (a :*: b), Constructor c)
    => GConstrToValue (C1 c (a :*: b)) where
    {-# INLINE gConstrToValue #-}
    gConstrToValue False s (M1 x) = runST (do
        marr <- A.newSmallArray (productSize (proxy# :: Proxy# (a :*: b))) undefined
        gWriteFields s marr 0 x
        gMergeFields (proxy# :: Proxy# (a :*: b)) marr)
    gConstrToValue True s (M1 x) =
        let !k = constrFmt s $ conName @c undefined
            !v = runST (do
                    marr <- A.newSmallArray (productSize (proxy# :: Proxy# (a :*: b))) undefined
                    gWriteFields s marr 0 x
                    gMergeFields (proxy# :: Proxy# (a :*: b)) marr)
        in Map (V.singleton (Str k, v))

--------------------------------------------------------------------------------
-- Data types
instance GConstrToValue f => GToValue (D1 c f) where
    {-# INLINE gToValue #-}
    gToValue s (M1 x) = gConstrToValue False s x

--------------------------------------------------------------------------------
-- MessagePack
--------------------------------------------------------------------------------

class GEncodeMessagePack f where
    gEncodeMessagePack :: Settings -> f a -> B.Builder ()

--------------------------------------------------------------------------------
-- Selectors

instance (GEncodeMessagePack f, Selector (MetaSel (Just l) u ss ds)) => GEncodeMessagePack (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gEncodeMessagePack #-}
    gEncodeMessagePack s m1@(M1 x) = (MB.str . fieldFmt s $ selName m1) >> gEncodeMessagePack s x

instance GEncodeMessagePack f => GEncodeMessagePack (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gEncodeMessagePack #-}
    gEncodeMessagePack s (M1 x) = gEncodeMessagePack s x

instance (GEncodeMessagePack a, GEncodeMessagePack b) => GEncodeMessagePack (a :*: b) where
    {-# INLINE gEncodeMessagePack #-}
    gEncodeMessagePack s (a :*: b) = gEncodeMessagePack s a >> gEncodeMessagePack s b

instance MessagePack a => GEncodeMessagePack (K1 i a) where
    {-# INLINE gEncodeMessagePack #-}
    gEncodeMessagePack _ (K1 x) = encodeMessagePack x

class GAddProductSize (f :: * -> *) where
    gAddProductSize :: Proxy# f -> Int -> B.Builder ()

instance GAddProductSize a => GAddProductSize (a :*: b) where
    {-# INLINE gAddProductSize #-}
    gAddProductSize _ = gAddProductSize (proxy# :: Proxy# a)

instance GAddProductSize (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gAddProductSize #-}
    gAddProductSize _ = MB.arrayHeader

instance GAddProductSize (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gAddProductSize #-}
    gAddProductSize _ = MB.mapHeader

--------------------------------------------------------------------------------
-- Constructors

class GConstrEncodeMessagePack f where
    gConstrEncodeMessagePack :: Bool -> Settings -> f a -> B.Builder ()

instance GConstrEncodeMessagePack V1 where
    {-# INLINE gConstrEncodeMessagePack #-}
    gConstrEncodeMessagePack _ _ _ = error "Z.Data.MessagePack.Base: empty data type"

instance (GConstrEncodeMessagePack f, GConstrEncodeMessagePack g) => GConstrEncodeMessagePack (f :+: g) where
    {-# INLINE gConstrEncodeMessagePack #-}
    gConstrEncodeMessagePack _ s (L1 x) = gConstrEncodeMessagePack True s x
    gConstrEncodeMessagePack _ s (R1 x) = gConstrEncodeMessagePack True s x

-- | Constructor without payload, convert to String
instance (Constructor c) => GConstrEncodeMessagePack (C1 c U1) where
    {-# INLINE gConstrEncodeMessagePack #-}
    -- There should be no chars need escaping in constructor name
    gConstrEncodeMessagePack _ s (M1 _) = MB.str . constrFmt s $ conName (undefined :: t c U1 a)

-- | Constructor with a single payload
instance (Constructor c, GEncodeMessagePack (S1 (MetaSel Nothing u ss ds) f))
    => GConstrEncodeMessagePack (C1 c (S1 (MetaSel Nothing u ss ds) f)) where
    {-# INLINE gConstrEncodeMessagePack #-}
    gConstrEncodeMessagePack False s (M1 x) = do
        gEncodeMessagePack s x
    gConstrEncodeMessagePack True s (M1 x) = do
        MB.mapHeader 1
        MB.str (constrFmt s $ conName @c undefined)
        gEncodeMessagePack s x

instance (Constructor c, GEncodeMessagePack (S1 (MetaSel (Just l) u ss ds) f))
    => GConstrEncodeMessagePack (C1 c (S1 (MetaSel (Just l) u ss ds) f)) where
    {-# INLINE gConstrEncodeMessagePack #-}
    gConstrEncodeMessagePack False s (M1 x) = do
        MB.mapHeader 1
        gEncodeMessagePack s x
    gConstrEncodeMessagePack True s (M1 x) = do
        MB.mapHeader 1
        MB.str (constrFmt s $ conName @c undefined)
        MB.mapHeader 1
        gEncodeMessagePack s x

-- | Constructor with multiple payloads
instance (GEncodeMessagePack (a :*: b), GAddProductSize (a :*: b), ProductSize (a :*: b), Constructor c)
    => GConstrEncodeMessagePack (C1 c (a :*: b)) where
    {-# INLINE gConstrEncodeMessagePack #-}
    gConstrEncodeMessagePack False s (M1 x) = do
        gAddProductSize (proxy# :: Proxy# (a :*: b)) (productSize (proxy# :: Proxy# (a :*: b)))
        gEncodeMessagePack s x
    gConstrEncodeMessagePack True s (M1 x) = do
        MB.mapHeader 1
        MB.str (constrFmt s $ conName @c @_ @_ @_ undefined)
        gAddProductSize (proxy# :: Proxy# (a :*: b)) (productSize (proxy# :: Proxy# (a :*: b)))
        gEncodeMessagePack s x

--------------------------------------------------------------------------------
-- Data types
instance GConstrEncodeMessagePack f => GEncodeMessagePack (D1 c f) where
    {-# INLINE gEncodeMessagePack #-}
    gEncodeMessagePack s (M1 x) = gConstrEncodeMessagePack False s x

--------------------------------------------------------------------------------
-- GFromValue
--------------------------------------------------------------------------------

class GFromValue f where
    gFromValue :: Settings -> Value -> Converter (f a)

--------------------------------------------------------------------------------
-- Selectors

type family LookupTable f where
    LookupTable (a :*: b) = LookupTable a
    LookupTable (S1 (MetaSel Nothing u ss ds) f) = V.Vector Value
    LookupTable (S1 (MetaSel (Just l) u ss ds) f) = FM.FlatMap Value Value

class GFromFields f where
    gFromFields :: Settings -> LookupTable f -> Int -> Converter (f a)

instance (ProductSize a, GFromFields a, GFromFields b, LookupTable a ~ LookupTable b)
    => GFromFields (a :*: b) where
    {-# INLINE gFromFields #-}
    gFromFields s v idx = do
        a <- gFromFields s v idx
        b <- gFromFields s v (idx + productSize (proxy# :: Proxy# a))
        pure (a :*: b)

instance (GFromValue f) => GFromFields (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gFromFields #-}
    gFromFields s v idx = do
        v' <- V.unsafeIndexM v idx
        M1 <$> gFromValue s v' <?> Index idx

instance (GFromValue f, Selector (MetaSel (Just l) u ss ds)) => GFromFields (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gFromFields #-}
    gFromFields s v _ = do
        case FM.lookup (Str fn) v of
            Just v' -> M1 <$> gFromValue s v' <?> Key fn
            _ | missingKeyAsNil s -> M1 <$> gFromValue s Nil <?> Key fn
              | otherwise -> fail' ("Z.Data.MessagePack.Base: missing field " <>  fn)
      where
        fn = (fieldFmt s) (selName (undefined :: S1 (MetaSel (Just l) u ss ds) f a))

instance GFromValue f => GFromValue (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gFromValue #-}
    gFromValue s x = M1 <$> gFromValue s x

instance (GFromValue f, Selector (MetaSel (Just l) u ss ds)) => GFromValue (S1 (MetaSel (Just l) u ss ds) f) where
    {-# INLINE gFromValue #-}
    gFromValue s (Map v) = do
        case FM.lookup (Str fn) (FM.packVectorR v) of
            Just v' -> M1 <$> gFromValue s v' <?> Key fn
            _ | missingKeyAsNil s -> M1 <$> gFromValue s Nil <?> Key fn
              | otherwise -> fail' ("Z.Data.MessagePack.Base: missing field " <>  fn)
      where fn = (fieldFmt s) (selName (undefined :: S1 (MetaSel (Just l) u ss ds) f a))
    gFromValue s v = typeMismatch ("field " <> fn) "Map" v <?> Key fn
      where fn = (fieldFmt s) (selName (undefined :: S1 (MetaSel (Just l) u ss ds) f a))

instance MessagePack a => GFromValue (K1 i a) where
    {-# INLINE gFromValue #-}
    gFromValue _ x = K1 <$> fromValue x

class GBuildLookup f where
    gBuildLookup :: Proxy# f -> Int -> T.Text -> Value -> Converter (LookupTable f)

instance (GBuildLookup a, GBuildLookup b) => GBuildLookup (a :*: b) where
    {-# INLINE gBuildLookup #-}
    gBuildLookup _ siz = gBuildLookup (proxy# :: Proxy# a) siz

instance GBuildLookup (S1 (MetaSel Nothing u ss ds) f) where
    {-# INLINE gBuildLookup #-}
    gBuildLookup _ siz name (Array v)
        -- we have to check size here to use 'unsafeIndexM' later
        | siz' /= siz = fail' . B.unsafeBuildText $ do
            "converting "
            T.text name
            " failed, product size mismatch, expected "
            T.int siz
            ", get"
            T.int siz'
        | otherwise = pure v
      where siz' = V.length v
    gBuildLookup _ _   name x         = typeMismatch name "Array" x

instance GBuildLookup (S1 ((MetaSel (Just l) u ss ds)) f) where
    {-# INLINE gBuildLookup #-}
    -- we don't check size, so that duplicated keys are preserved
    gBuildLookup _ _ _ (Map v) = pure $! FM.packVectorR v
    gBuildLookup _ _ name x    = typeMismatch name "Map" x

--------------------------------------------------------------------------------
-- Constructors

class GConstrFromValue f where
    gConstrFromValue :: Bool    -- ^ Is this a sum type(more than one constructor)?
                     -> Settings -> Value -> Converter (f a)

instance GConstrFromValue V1 where
    {-# INLINE gConstrFromValue #-}
    gConstrFromValue _ _ _ = error "Z.Data.MessagePack.Base: empty data type"

instance (GConstrFromValue f, GConstrFromValue g) => GConstrFromValue (f :+: g) where
    {-# INLINE gConstrFromValue #-}
    gConstrFromValue _ s x = (L1 <$> gConstrFromValue True s x) <|> (R1 <$> gConstrFromValue True s x)

-- | Constructor without payload, convert to String
instance (Constructor c) => GConstrFromValue (C1 c U1) where
    {-# INLINE gConstrFromValue #-}
    gConstrFromValue _ s (Str x)
        | cn == x   = pure (M1 U1)
        | otherwise = fail' . T.concat $ ["converting ", cn', "failed, unknown constructor name ", x]
      where cn = constrFmt s $ conName (undefined :: t c U1 a)
            cn' = T.pack $ conName (undefined :: t c U1 a)
    gConstrFromValue _ _ v = typeMismatch cn' "String" v
      where cn' = T.pack $ conName (undefined :: t c U1 a)

-- | Constructor with a single payload
instance (Constructor c, GFromValue (S1 sc f)) => GConstrFromValue (C1 c (S1 sc f)) where
    {-# INLINE gConstrFromValue #-}
    -- | Single constructor
    gConstrFromValue False s x = M1 <$> gFromValue s x
    gConstrFromValue True s x = case x of
        Map v -> case V.indexM v 0 of
            Just (Str k, v')
                | k == cn -> M1 <$> gFromValue s v' <?> Key cn
            _             -> fail' .T.concat $ ["converting ", cn', " failed, constructor not found"]
        _ ->  typeMismatch cn' "Map" x
      where cn = constrFmt s $ conName @c undefined
            cn' = T.pack $ conName @c undefined

-- | Constructor with multiple payloads
instance (ProductSize (a :*: b), GFromFields (a :*: b), GBuildLookup (a :*: b), Constructor c)
    => GConstrFromValue (C1 c (a :*: b)) where
    {-# INLINE gConstrFromValue #-}
    gConstrFromValue False s x = do
        t <- gBuildLookup p (productSize p) cn' x
        M1 <$> gFromFields s t 0
      where cn' = T.pack $ conName @c undefined
            p = proxy# :: Proxy# (a :*: b)
    gConstrFromValue True s x = case x of
        Map v -> case V.indexM v 0 of
            Just (Str k, v')
                | k == cn -> do t <- gBuildLookup p (productSize p) cn' v'
                                M1 <$> gFromFields s t 0
            _             -> fail' .T.concat $ ["converting ", cn', " failed, constructor not found"]
        _ ->  typeMismatch cn' "Map" x
      where cn = constrFmt s $ conName @c undefined
            cn' = T.pack $ conName @c undefined
            p = proxy# :: Proxy# (a :*: b)

--------------------------------------------------------------------------------
-- Data types
instance GConstrFromValue f => GFromValue (D1 c f) where
    {-# INLINE gFromValue #-}
    gFromValue s x = M1 <$> gConstrFromValue False s x

--------------------------------------------------------------------------------
-- Built-in Instances
--------------------------------------------------------------------------------
-- | Use 'Nil' as @Proxy a@
instance MessagePack (Proxy a) where
    {-# INLINE fromValue #-}; fromValue = fromNil "Proxy" Proxy;
    {-# INLINE toValue #-}; toValue _ = Nil;
    {-# INLINE encodeMessagePack #-}; encodeMessagePack _ = MB.nil;

instance MessagePack Value   where
    {-# INLINE fromValue #-}; fromValue = pure;
    {-# INLINE toValue #-}; toValue = id;
    {-# INLINE encodeMessagePack #-}; encodeMessagePack = MB.value;

instance MessagePack T.Text   where
    {-# INLINE fromValue #-}; fromValue = withStr "Text" pure;
    {-# INLINE toValue #-}; toValue = Str;
    {-# INLINE encodeMessagePack #-}; encodeMessagePack = MB.str;

-- | Note this instance doesn't reject large input
instance MessagePack Scientific where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "Data.Scientific.Scientific" pure
    {-# INLINE toValue #-}
    toValue x = MB.scientificValue (coefficient x) (fromIntegral $ base10Exponent x)
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack x = MB.scientific (coefficient x) (fromIntegral $ base10Exponent x)

-- | default instance prefer later key
instance (Ord a, MessagePack a, MessagePack b) => MessagePack (FM.FlatMap a b) where
    {-# INLINE fromValue #-}
    fromValue = withFlatMapR "Z.Data.Vector.FlatMap.FlatMap" $ \ m ->
        let kvs = V.unpack (FM.sortedKeyValues m)
        in FM.packR <$> (forM kvs $ \ (k, v) -> do
            k' <- fromValue k
            v' <- fromValue v <?> Key (T.toText k)
            return (k', v'))
    {-# INLINE toValue #-}
    toValue = Map . V.map (\ (k, v) -> (toValue k, toValue v)) . FM.sortedKeyValues
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.map encodeMessagePack encodeMessagePack . FM.sortedKeyValues

instance (Ord a, MessagePack a) => MessagePack (FS.FlatSet a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Vector.FlatSet.FlatSet" $ \ vs ->
        FS.packRN (V.length vs) <$>
            (zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs))
    {-# INLINE toValue #-}
    toValue = Array . V.map' toValue . FS.sortedValues
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack . FS.sortedValues

-- | default instance prefer later key
instance (Eq a, Hashable a, MessagePack a, MessagePack b) => MessagePack (HM.HashMap a b) where
    {-# INLINE fromValue #-}
    fromValue = withKeyValues "Data.HashMap.HashMap" $ \ kvs ->
        HM.fromList <$> (forM (V.unpack kvs) $ \ (k, v) -> do
            !k' <- fromValue k
            !v' <- fromValue v <?> Key (T.toText k)
            return (k', v'))
    {-# INLINE toValue #-}
    toValue = Map . V.pack . map (\ (k,v) -> (toValue k, toValue v)) . HM.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.map' encodeMessagePack encodeMessagePack . HM.toList

instance (Ord a, MessagePack a, MessagePack b) => MessagePack (M.Map a b) where
    {-# INLINE fromValue #-}
    fromValue = withKeyValues "Data.HashMap.HashMap" $ \ kvs ->
        M.fromList <$> (forM (V.unpack kvs) $ \ (k, v) -> do
            !k' <- fromValue k
            !v' <- fromValue v <?> Key (T.toText k)
            return (k', v'))
    {-# INLINE toValue #-}
    toValue = Map . V.pack . map (\ (k,v) -> (toValue k, toValue v)) . M.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.map' encodeMessagePack encodeMessagePack . M.toList

instance MessagePack a => MessagePack (FIM.FlatIntMap a) where
    {-# INLINE fromValue #-}
    fromValue = withFlatMapR "Z.Data.Vector.FlatIntMap.FlatIntMap" $ \ m ->
        let kvs = FM.sortedKeyValues m
        in FIM.packVectorR <$> (forM kvs $ \ (k, v) -> do
            case k of
                Int k' -> do
                    v' <- fromValue v <?> Key (T.toText k)
                    return (V.IPair (fromIntegral k') v')
                _ -> fail' ("converting Z.Data.Vector.FlatIntMap.FlatIntMap failed, unexpected key " <> (T.toText k)))
    {-# INLINE toValue #-}
    toValue = Map . V.map' toKV . FIM.sortedKeyValues
      where toKV (V.IPair i x) = let !k = Int (fromIntegral i)
                                     !v = toValue x
                                 in (k, v)
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack m = do
        let kvs = FIM.sortedKeyValues m
        MB.mapHeader (V.length kvs)
        V.traverseVec_ (\ (V.IPair k v) -> MB.int (fromIntegral k) >> encodeMessagePack v) kvs

instance MessagePack a => MessagePack (IM.IntMap a) where
    {-# INLINE fromValue #-}
    fromValue = withKeyValues "Data.IntMap.IntMap" $ \ kvs ->
        IM.fromList <$> (forM (V.unpack kvs) $ \ (k, v) -> do
            case k of
                Int k' -> do
                    v' <- fromValue v <?> Key (T.toText k)
                    return (fromIntegral k', v')
                _ -> fail' ("converting Data.IntMap.IntMap failed, unexpected key " <> (T.toText k)))
    {-# INLINE toValue #-}
    toValue = Map . V.pack . map toKV . IM.toList
      where toKV (i, x) = let !k = Int (fromIntegral i)
                              !v = toValue x
                          in (k, v)
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack m = do
        MB.mapHeader (IM.size m)
        mapM_ (\ (k, v) -> MB.int (fromIntegral k) >> encodeMessagePack v) (IM.toList m)

instance MessagePack FIS.FlatIntSet where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Vector.FlatIntSet.FlatIntSet" $ \ vs ->
        FIS.packRN (V.length vs) <$> zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs)
    {-# INLINE toValue #-}
    toValue = toValue . FIS.sortedValues
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . FIS.sortedValues

instance MessagePack IS.IntSet where
    {-# INLINE fromValue #-}
    fromValue = withArray "Data.IntSet.IntSet" $ \ vs ->
        IS.fromList <$> zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs)
    {-# INLINE toValue #-}
    toValue = toValue . IS.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . IS.toList

instance (Ord a, MessagePack a) => MessagePack (Set.Set a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Data.Set.Set" $ \ vs ->
        Set.fromList <$> zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs)
    {-# INLINE toValue #-}
    toValue = toValue . Set.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . Set.toList

instance MessagePack a => MessagePack (Seq.Seq a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Data.Seq.Seq" $ \ vs ->
        Seq.fromList <$> zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs)
    {-# INLINE toValue #-}
    toValue = toValue . Foldable.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . Foldable.toList

instance MessagePack a => MessagePack (Tree.Tree a) where
    {-# INLINE fromValue #-}
    fromValue = withFlatMapR "Data.Tree" $ \obj -> do
        !n <- obj .: "rootLabel"
        !d <- obj .: "subForest"
        pure (Tree.Node n d)
    {-# INLINE toValue #-}
    toValue x = object [ "rootLabel" .= (Tree.rootLabel x) , "subForest" .= (Tree.subForest x) ]
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack x = object' ( "rootLabel" .! (Tree.rootLabel x) <> "subForest" .! (Tree.subForest x) )

instance MessagePack a => MessagePack (A.Array a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Array.Array"
        (V.traverseWithIndex $ \ k v -> fromValue v <?> Index k)
    {-# INLINE toValue #-}
    toValue = Array . V.map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack

instance MessagePack a => MessagePack (A.SmallArray a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Array.SmallArray"
        (V.traverseWithIndex $ \ k v -> fromValue v <?> Index k)
    {-# INLINE toValue #-}
    toValue = Array . V.map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack

instance (Prim a, MessagePack a) => MessagePack (A.PrimArray a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Array.PrimArray"
        (V.traverseWithIndex $ \ k v -> fromValue v <?> Index k)
    {-# INLINE toValue #-}
    toValue = Array . V.map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack

instance (A.PrimUnlifted a, MessagePack a) => MessagePack (A.UnliftedArray a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Array.UnliftedArray"
        (V.traverseWithIndex $ \ k v -> fromValue v <?> Index k)
    {-# INLINE toValue #-}
    toValue = Array . V.map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack

instance MessagePack A.ByteArray where
    {-# INLINE fromValue #-}
    fromValue = withBin "ByteArray" $ \ (V.PrimVector pa@(A.PrimArray ba#) s l) ->
        if A.sizeofArr pa == l && s == 0
        then pure (A.ByteArray ba#)
        else pure $! A.cloneByteArray (A.ByteArray ba#) s l
    {-# INLINE toValue #-}
    toValue (A.ByteArray ba#) = Bin (V.arrVec (A.PrimArray ba#))
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack (A.ByteArray ba#) = MB.bin (V.arrVec (A.PrimArray ba#))

instance (Prim a, MessagePack a) => MessagePack (V.PrimVector a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Vector.PrimVector"
        (V.traverseWithIndex $ \ k v -> fromValue v <?> Index k)
    {-# INLINE toValue #-}
    toValue = Array . V.map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack

instance MessagePack a => MessagePack (V.Vector a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Vector.Vector"
        (V.traverseWithIndex $ \ k v -> fromValue v <?> Index k)
    {-# INLINE toValue #-}
    toValue = Array . V.map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array encodeMessagePack

instance (Eq a, Hashable a, MessagePack a) => MessagePack (HS.HashSet a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "Z.Data.Vector.FlatSet.FlatSet" $ \ vs ->
        HS.fromList <$>
            (zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs))
    {-# INLINE toValue #-}
    toValue = toValue . HS.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . HS.toList

instance MessagePack a => MessagePack [a] where
    {-# INLINE fromValue #-}
    fromValue = withArray "[a]" $ \ vs ->
        zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs)
    {-# INLINE toValue #-}
    toValue = Array . V.pack . map toValue
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.array' encodeMessagePack

instance MessagePack a => MessagePack (NonEmpty a) where
    {-# INLINE fromValue #-}
    fromValue = withArray "NonEmpty" $ \ vs -> do
        l <- zipWithM (\ k v -> fromValue v <?> Index k) [0..] (V.unpack vs)
        case l of (x:xs) -> pure (x :| xs)
                  _      -> fail' "unexpected empty array"
    {-# INLINE toValue #-}
    toValue = toValue . NonEmpty.toList
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . NonEmpty.toList

instance MessagePack Bool where
    {-# INLINE fromValue #-}; fromValue = withBool "Bool" pure;
    {-# INLINE toValue #-}; toValue = Bool;
    {-# INLINE encodeMessagePack #-}; encodeMessagePack = MB.bool

instance MessagePack Char where
    {-# INLINE fromValue #-}
    fromValue = withStr "Char" $ \ t ->
        if (T.length t == 1)
        then pure (T.head t)
        else fail' (T.concat ["converting Char failed, expected a string of length 1"])
    {-# INLINE toValue #-}
    toValue = Str . T.singleton
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.str . T.singleton

instance MessagePack Double where
    {-# INLINE fromValue #-}
    fromValue (Float d) = pure $! realToFrac d
    fromValue (Double d) = pure d
    fromValue v = typeMismatch "Double" "Float or Double" v
    {-# INLINE toValue #-}; toValue = Double;
    {-# INLINE encodeMessagePack #-}; encodeMessagePack = MB.double;

instance MessagePack Float  where
    {-# INLINE fromValue #-};
    fromValue (Float d) = pure d
    fromValue (Double d) = pure $! realToFrac d
    fromValue v = typeMismatch "Float" "Float or Double" v
    {-# INLINE toValue #-}; toValue = Float;
    {-# INLINE encodeMessagePack #-}; encodeMessagePack = MB.float;

#define INT_MessagePack_INSTANCE(typ) \
    instance MessagePack typ where \
        {-# INLINE fromValue #-}; \
            fromValue (Int x) = pure $! fromIntegral x; \
            fromValue v = typeMismatch "##typ##" "Int" v; \
        {-# INLINE toValue #-}; toValue = Int . fromIntegral; \
        {-# INLINE encodeMessagePack #-}; encodeMessagePack = MB.int . fromIntegral;
INT_MessagePack_INSTANCE(Int   )
INT_MessagePack_INSTANCE(Int8  )
INT_MessagePack_INSTANCE(Int16 )
INT_MessagePack_INSTANCE(Int32 )
INT_MessagePack_INSTANCE(Int64 )
INT_MessagePack_INSTANCE(Word  )
INT_MessagePack_INSTANCE(Word8 )
INT_MessagePack_INSTANCE(Word16)
INT_MessagePack_INSTANCE(Word32)
INT_MessagePack_INSTANCE(Word64)

instance MessagePack Integer where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "Integer" $ \ n ->
        case Sci.floatingOrInteger n :: Either Double Integer of
            Right x -> pure x
            Left _  -> fail' . B.unsafeBuildText $ do
                "converting Integer failed, unexpected floating number "
                T.scientific n
    {-# INLINE toValue #-}
    toValue x = MB.scientificValue x 0
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack x = MB.scientific x 0

instance MessagePack Natural where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "Natural" $ \ n ->
        if n < 0
        then fail' . B.unsafeBuildText $ do
                "converting Natural failed, unexpected negative number "
                T.scientific n
        else case Sci.floatingOrInteger n :: Either Double Natural of
            Right x -> pure x
            Left _  -> fail' . B.unsafeBuildText $ do
                "converting Natural failed, unexpected floating number "
                T.scientific n
    {-# INLINE toValue #-}
    toValue x = MB.scientificValue (fromIntegral x) 0
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack x = MB.scientific (fromIntegral x) 0

instance MessagePack Ordering where
    {-# INLINE fromValue #-}
    fromValue = withStr "Ordering" $ \ s ->
        case s of
            "LT" -> pure LT
            "EQ" -> pure EQ
            "GT" -> pure GT
            _ -> fail' . T.concat $ ["converting Ordering failed, unexpected ",
                                        s, " expected \"LT\", \"EQ\", or \"GT\""]
    {-# INLINE toValue #-}
    toValue LT = Str "LT"
    toValue EQ = Str "EQ"
    toValue GT = Str "GT"
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack LT = MB.str "LT"
    encodeMessagePack EQ = MB.str "EQ"
    encodeMessagePack GT = MB.str "GT"

instance MessagePack () where
    {-# INLINE fromValue #-}
    fromValue = withArray "()" $ \ v ->
        if V.null v
        then pure ()
        else fail' "converting () failed, expected an empty array"
    {-# INLINE toValue #-}
    toValue () = Array V.empty
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack () = MB.arrayHeader 0

instance MessagePack ExitCode where
    {-# INLINE fromValue #-}
    fromValue (Str "ExitSuccess") = return ExitSuccess
    fromValue (Int x) = return (ExitFailure (fromIntegral x))
    fromValue _ =  fail' "converting ExitCode failed, expected a string or number"

    {-# INLINE toValue #-}
    toValue ExitSuccess     = Str "ExitSuccess"
    toValue (ExitFailure n) = Int (fromIntegral n)

    {-# INLINE encodeMessagePack #-}
    encodeMessagePack ExitSuccess     = MB.str "ExitSuccess"
    encodeMessagePack (ExitFailure n) = B.int n

instance MessagePack Version where
    {-# INLINE fromValue #-}
    fromValue = withStr "Version" (go . readP_to_S parseVersion . T.unpack)
      where
        go [(v,[])] = pure v
        go (_ : xs) = go xs
        go _        = fail "converting Version failed"
    {-# INLINE toValue #-}
    toValue = Str . T.pack . show
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = MB.str' . show

instance MessagePack a => MessagePack (Maybe a) where
    {-# INLINE fromValue #-}
    fromValue Nil = pure Nothing
    fromValue v    = Just <$> fromValue v
    {-# INLINE toValue #-}
    toValue Nothing  = Nil
    toValue (Just x) = toValue x
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack Nothing  = MB.nil
    encodeMessagePack (Just x) = encodeMessagePack x

instance (MessagePack a, Integral a) => MessagePack (Ratio a) where
    {-# INLINE fromValue #-}
    fromValue = withFlatMapR "Rational" $ \obj -> do
        !n <- obj .: "numerator"
        !d <- obj .: "denominator"
        if d == 0
        then fail' "Ratio denominator was 0"
        else pure (n % d)
    {-# INLINE toValue #-}
    toValue x = object [ "numerator" .= (numerator x) , "denominator" .= (denominator x) ]
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack x = object' ( "numerator" .! (numerator x) <> "denominator" .! (denominator x) )

instance HasResolution a => MessagePack (Fixed a) where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "Data.Fixed" $ pure . realToFrac
    {-# INLINE toValue #-}
    toValue = toValue @Scientific . realToFrac
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack @Scientific . realToFrac

--------------------------------------------------------------------------------

-- | MessagePack extension type @Ext 0xFF@
instance MessagePack UTCTime where
    {-# INLINE fromValue #-}
    fromValue = withSystemTime "UTCTime" $ pure . systemToUTCTime
    {-# INLINE toValue #-}
    toValue t = let (MkSystemTime s ns) = utcToSystemTime t in MB.timestampValue s (fromIntegral ns)
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack t = let (MkSystemTime s ns) = utcToSystemTime t in MB.timestamp s (fromIntegral ns)

-- | MessagePack extension type @Ext 0xFF@
instance MessagePack SystemTime where
    {-# INLINE fromValue #-}
    fromValue = withSystemTime "UTCTime" $ pure
    {-# INLINE toValue #-}
    toValue (MkSystemTime s ns) = MB.timestampValue s (fromIntegral ns)
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack (MkSystemTime s ns) = MB.timestamp s (fromIntegral ns)

-- | @YYYY-MM-DDTHH:MM:SS.SSSZ@
instance MessagePack ZonedTime where
    {-# INLINE fromValue #-}
    fromValue = withStr "ZonedTime" $ \ t ->
        case P.parse' (P.zonedTime <* P.endOfInput) (T.getUTF8Bytes t) of
            Left err -> fail' $ "could not parse date as ZonedTime: " <> T.toText err
            Right r  -> return r
    {-# INLINE toValue #-}
    toValue t = Str (B.unsafeBuildText (B.zonedTime t))
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack t = MB.str (B.unsafeBuildText (B.zonedTime t))

-- | @YYYY-MM-DD@
instance MessagePack Day where
    {-# INLINE fromValue #-}
    fromValue = withStr "Day" $ \ t ->
        case P.parse' (P.day <* P.endOfInput) (T.getUTF8Bytes t) of
            Left err -> fail' $ "could not parse date as Day: " <> T.toText err
            Right r  -> return r
    {-# INLINE toValue #-}
    toValue t = Str (B.unsafeBuildText (B.day t))
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack t = MB.str (B.unsafeBuildText (B.day t))

-- | @YYYY-MM-DDTHH:MM:SS.SSSZ@
instance MessagePack LocalTime where
    {-# INLINE fromValue #-}
    fromValue = withStr "LocalTime" $ \ t ->
        case P.parse' (P.localTime <* P.endOfInput) (T.getUTF8Bytes t) of
            Left err -> fail' $ "could not parse date as LocalTime: " <> T.toText err
            Right r  -> return r
    {-# INLINE toValue #-}
    toValue t = Str (B.unsafeBuildText (B.localTime t))
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack t = MB.str (B.unsafeBuildText (B.localTime t))

-- | @HH:MM:SS.SSS@
instance MessagePack TimeOfDay where
    {-# INLINE fromValue #-}
    fromValue = withStr "TimeOfDay" $ \ t ->
        case P.parse' (P.timeOfDay <* P.endOfInput) (T.getUTF8Bytes t) of
            Left err -> fail' $ "could not parse time as TimeOfDay: " <> T.toText err
            Right r  -> return r
    {-# INLINE toValue #-}
    toValue t = Str (B.unsafeBuildText (B.timeOfDay t))
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack t = MB.str (B.unsafeBuildText (B.timeOfDay t))

instance MessagePack NominalDiffTime where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "NominalDiffTime" $ pure . realToFrac
    {-# INLINE toValue #-}
    toValue = toValue @Scientific . realToFrac
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack @Scientific . realToFrac

instance MessagePack DiffTime where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "DiffTime" $ pure . realToFrac
    {-# INLINE toValue #-}
    toValue = toValue @Scientific . realToFrac
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack @Scientific . realToFrac

instance MessagePack CalendarDiffTime where
    {-# INLINE fromValue #-}
    fromValue = withFlatMapR "CalendarDiffTime" $ \ v ->
        CalendarDiffTime <$> v .: "months" <*> v .: "time"
    {-# INLINE toValue #-}
    toValue (CalendarDiffTime m nt) = object [ "months" .= m , "time" .= nt ]
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack (CalendarDiffTime m nt) = object' ("months" .! m <> "time" .! nt)

instance MessagePack CalendarDiffDays where
    {-# INLINE fromValue #-}
    fromValue = withFlatMapR "CalendarDiffDays" $ \ v ->
        CalendarDiffDays <$> v .: "months" <*> v .: "days"
    {-# INLINE toValue #-}
    toValue (CalendarDiffDays m d) = object ["months" .= m, "days" .= d]
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack (CalendarDiffDays m d) = object' ("months" .! m <> "days" .! d)

instance MessagePack DayOfWeek where
    {-# INLINE fromValue #-}
    fromValue (Str "monday"   ) = pure Monday
    fromValue (Str "tuesday"  ) = pure Tuesday
    fromValue (Str "wednesday") = pure Wednesday
    fromValue (Str "thursday" ) = pure Thursday
    fromValue (Str "friday"   ) = pure Friday
    fromValue (Str "saturday" ) = pure Saturday
    fromValue (Str "sunday"   ) = pure Sunday
    fromValue (Str _   )        = fail' "converting DayOfWeek failed, value should be one of weekdays"
    fromValue v                 = typeMismatch "DayOfWeek" "String" v
    {-# INLINE toValue #-}
    toValue Monday    = Str "monday"
    toValue Tuesday   = Str "tuesday"
    toValue Wednesday = Str "wednesday"
    toValue Thursday  = Str "thursday"
    toValue Friday    = Str "friday"
    toValue Saturday  = Str "saturday"
    toValue Sunday    = Str "sunday"
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack Monday    = MB.str "monday"
    encodeMessagePack Tuesday   = MB.str "tuesday"
    encodeMessagePack Wednesday = MB.str "wednesday"
    encodeMessagePack Thursday  = MB.str "thursday"
    encodeMessagePack Friday    = MB.str "friday"
    encodeMessagePack Saturday  = MB.str "saturday"
    encodeMessagePack Sunday    = MB.str "sunday"

--------------------------------------------------------------------------------

deriving newtype instance MessagePack (f (g a)) => MessagePack (Compose f g a)
deriving newtype instance MessagePack a => MessagePack (Semigroup.Min a)
deriving newtype instance MessagePack a => MessagePack (Semigroup.Max a)
deriving newtype instance MessagePack a => MessagePack (Semigroup.First a)
deriving newtype instance MessagePack a => MessagePack (Semigroup.Last a)
deriving newtype instance MessagePack a => MessagePack (Semigroup.WrappedMonoid a)
deriving newtype instance MessagePack a => MessagePack (Semigroup.Dual a)
deriving newtype instance MessagePack a => MessagePack (Monoid.First a)
deriving newtype instance MessagePack a => MessagePack (Monoid.Last a)
deriving newtype instance MessagePack a => MessagePack (Identity a)
deriving newtype instance MessagePack a => MessagePack (Const a b)
deriving newtype instance MessagePack b => MessagePack (Tagged a b)

--------------------------------------------------------------------------------

deriving newtype instance MessagePack CChar
deriving newtype instance MessagePack CSChar
deriving newtype instance MessagePack CUChar
deriving newtype instance MessagePack CShort
deriving newtype instance MessagePack CUShort
deriving newtype instance MessagePack CInt
deriving newtype instance MessagePack CUInt
deriving newtype instance MessagePack CLong
deriving newtype instance MessagePack CULong
deriving newtype instance MessagePack CPtrdiff
deriving newtype instance MessagePack CSize
deriving newtype instance MessagePack CWchar
deriving newtype instance MessagePack CSigAtomic
deriving newtype instance MessagePack CLLong
deriving newtype instance MessagePack CULLong
deriving newtype instance MessagePack CBool
deriving newtype instance MessagePack CIntPtr
deriving newtype instance MessagePack CUIntPtr
deriving newtype instance MessagePack CIntMax
deriving newtype instance MessagePack CUIntMax
deriving newtype instance MessagePack CClock
deriving newtype instance MessagePack CTime
deriving newtype instance MessagePack CUSeconds
deriving newtype instance MessagePack CSUSeconds
deriving newtype instance MessagePack CFloat
deriving newtype instance MessagePack CDouble

--------------------------------------------------------------------------------

deriving anyclass instance (MessagePack (f a), MessagePack (g a), MessagePack a) => MessagePack (Sum f g a)
deriving anyclass instance (MessagePack a, MessagePack b) => MessagePack (Either a b)
deriving anyclass instance (MessagePack (f a), MessagePack (g a)) => MessagePack (Product f g a)

deriving anyclass instance (MessagePack a, MessagePack b) => MessagePack (a, b)
deriving anyclass instance (MessagePack a, MessagePack b, MessagePack c) => MessagePack (a, b, c)
deriving anyclass instance (MessagePack a, MessagePack b, MessagePack c, MessagePack d) => MessagePack (a, b, c, d)
deriving anyclass instance (MessagePack a, MessagePack b, MessagePack c, MessagePack d, MessagePack e) => MessagePack (a, b, c, d, e)
deriving anyclass instance (MessagePack a, MessagePack b, MessagePack c, MessagePack d, MessagePack e, MessagePack f) => MessagePack (a, b, c, d, e, f)
deriving anyclass instance (MessagePack a, MessagePack b, MessagePack c, MessagePack d, MessagePack e, MessagePack f, MessagePack g) => MessagePack (a, b, c, d, e, f, g)
