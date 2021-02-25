{-# OPTIONS_GHC -fno-warn-orphans #-}

{-|
Module      : Z.Data.MessagePack
Description : Fast MessagePack serialization/deserialization
Copyright   : (c) Dong Han, 2019
License     : BSD
Maintainer  : winterland1989@gmail.com
Stability   : experimental
Portability : non-portable

This module provides an interface similar to "Z.Data.JSON", to work with MessagePack binary format.

  * @Maybe a@ convert to 'Nil' in 'Nothing' case, and @a@ in 'Just' case.
  * Use 'Int64'(signed) or 'Word64'(unsigned) type to marshall int type format, smaller types will sliently truncate when overflow.
  * Use 'Double' to marshall float type format, 'Float' may lost precision.
  * Use 'Scientific' to marshall 'Ext' @0x00\/0x01@ type.
  * Use 'SystemTime' to marshall 'Ext' @0xFF@ type.
  * Record's field label are preserved.

  * We use MessagePack extension type -1 to encode\/decode 'SystemTime' and 'UTCTime':

        +--------+--------+--------+-----------------------------------+------------------------------+
        |  0xc7  |   12   |   -1   |nanoseconds in 32-bit unsigned int | seconds in 64-bit signed int |
        +--------+--------+--------+-----------------------------------+------------------------------+

  * We deliberately use ext type 0x00(positive) and 0x01(negative) to represent large numbers('Integer', 'Scientific', 'Fixed', 'DiffTime'...):

        +--------+--------+--------+-----------------------------------------+---------------------------------------+
        |  0xc7  |XXXXXXXX|  0x00  | base10 exponent(MessagePack int format) | coefficient(big endian 256-base limbs |
        +--------+--------+--------+-----------------------------------------+---------------------------------------+

        Use a MessagePack implementation supporting ext type to marshall it, result value is coefficient * (10 ^ exponent).

The easiest way to use the library is to define target data type, deriving 'GHC.Generics.Generic' and 'MessagePack' instances, e.g.

@
{-# LANGUAGE DeriveGeneric, DeriveAnyClass, DerivingStrategies #-}

import GHC.Generics (Generic)
import qualified Z.Data.MessagePack as MessagePack
import qualified Z.Data.Text as T

data Person = Person {name :: T.Text, age :: Int}
    deriving (Show, Generic)
    deriving anyclass (MessagePack.MessagePack)

> MessagePack.encode Person{ name=\"Alice\", age=16 }
> [130,164,110,97,109,101,165,65,108,105,99,101,163,97,103,101,16]
@

MessagePack is a schemaless format, which means the encoded data can be recovered into some form('Value' in haskell case)
without providing data definition, e.g. the data encoded above:

> [130,   164,   110,   97,   109,   101,   165,   65,   108,   105,   99,   101,   163,   97,   103,   101,   16]
>  0x82   0xA4   'n'    'a'   'm'    'e'    0xA5   'A'   'l'    'i'    'c'   'e'    0xA3   'a'   'g'    'e'    int
>  map    str                               str                                     str                        16
>  2kvs   4bytes                            5bytes                                  3bytes


This property makes it suitable for passing data across language boundary, e.g. from a static typed language to a dynamic one, at the cost of a lower space efficiency(i.e. type tag and field label).

-}

module Z.Data.MessagePack
  ( -- * MessagePack Class
    MessagePack(..), Value(..), defaultSettings, Settings(..), JSON.snakeCase, JSON.trainCase
    -- * Encode & Decode
  , readMessagePackFile, writeMessagePackFile
  , decode, decode', decodeChunks, encode, encodeChunks
  , DecodeError, ParseError
    -- * parse into MessagePack Value
  , parseValue, parseValue', parseValueChunks, parseValueChunks'
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

import           Data.Char
import           Data.Functor.Compose
import           Data.Functor.Const
import           Data.Functor.Identity
import           Data.Functor.Product
import           Data.Functor.Sum
import qualified Data.Monoid                    as Monoid
import           Data.Proxy                     (Proxy (..))
import           Data.Scientific                (Scientific, toBoundedInteger)
import qualified Data.Semigroup                 as Semigroup
import           Data.Tagged                    (Tagged (..))
import           Data.Time                      (Day, DiffTime, LocalTime, NominalDiffTime,
                                                TimeOfDay, UTCTime, ZonedTime)
import           Data.Time.Calendar             (CalendarDiffDays (..), DayOfWeek (..))
import           Data.Time.LocalTime            (CalendarDiffTime (..))
import           Data.Time.Clock.System         (SystemTime (..), utcToSystemTime, systemToUTCTime)
import           Data.Version                   (Version(versionBranch), makeVersion)
import           Foreign.C.Types
import           System.Exit                    (ExitCode(..))
import qualified Z.Data.Builder                 as B
import           Z.Data.MessagePack.Base
import qualified Z.Data.MessagePack.Builder     as MB
import qualified Z.Data.JSON                    as JSON
import qualified Z.Data.Parser                  as P
import qualified Z.Data.Text                    as T
import           Z.Data.CBytes            (CBytes)
import           Z.IO
import qualified Z.IO.FileSystem as FS

-- | Decode a 'MessagePack' instance from file.
readMessagePackFile :: (HasCallStack, MessagePack a) => CBytes -> IO a
readMessagePackFile p = unwrap "EPARSE" . decode' =<< FS.readFile p

-- | Encode a 'MessagePack' instance to file.
writeMessagePackFile :: (HasCallStack, MessagePack a) => CBytes -> a -> IO ()
writeMessagePackFile p x = FS.writeFile p (encode x)

--------------------------------------------------------------------------------

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

-- | Only round trip 'versionBranch' as MessagePack array.
instance MessagePack Version where
    {-# INLINE fromValue #-}
    fromValue v = makeVersion <$> fromValue v
    {-# INLINE toValue #-}
    toValue = toValue . versionBranch
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack . versionBranch

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

-- | This instance includes a bounds check to prevent maliciously
-- large inputs to fill up the memory of the target system. You can
-- newtype 'NominalDiffTime' and provide your own instance using
-- 'withScientific' if you want to allow larger inputs.
instance MessagePack NominalDiffTime where
    {-# INLINE fromValue #-}
    fromValue = withBoundedScientific "NominalDiffTime" $ pure . realToFrac
    {-# INLINE toValue #-}
    toValue = toValue @Scientific . realToFrac
    {-# INLINE encodeMessagePack #-}
    encodeMessagePack = encodeMessagePack @Scientific . realToFrac

-- | This instance includes a bounds check to prevent maliciously
-- large inputs to fill up the memory of the target system. You can
-- newtype 'DiffTime' and provide your own instance using
-- 'withScientific' if you want to allow larger inputs.
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
