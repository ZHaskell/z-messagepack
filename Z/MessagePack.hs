{-|
Module      : Z.MessagePack
Description : Fast MessagePack serialization/deserialization
Copyright   : (c) Dong Han, 2019
License     : BSD
Maintainer  : winterland1989@gmail.com
Stability   : experimental
Portability : non-portable

This module provides an interface similar to 'Z.Data.JSON', to work with MessagePack binary format.

  * @Maybe a@ convert to 'Nil' in 'Nothing' case, and @a@ in 'Just' case.
  * Use 'Int64'(signed) or 'Word64'(unsigned) type to marshall int type format, smaller types will sliently truncate when overflow.
  * Use 'Double' to marshall float type format, 'Float' may lost precision.
  * Use 'Scientific' to marshall 'Ext' @0x00@ type.
  * Use 'SystemTime' to marshall 'Ext' @0xFF@ type.
  * Record's field label are preserved.

  * We use MessagePack extension type -1 to encode\/decode 'SystemTime' and 'UTCTime':

    +--------+--------+--------+--------+--------+--------+--------+
    |  0xc7  |   12   |   -1   |nanoseconds in 32-bit unsigned int |
    +--------+--------+--------+--------+--------+--------+--------+
    +--------+--------+--------+--------+--------+--------+--------+--------+
                        seconds in 64-bit signed int                        |
    +--------+--------+--------+--------+--------+--------+--------+--------+

  * We deliberately use ext type 0 to represent large numbers(Integers, Scientific, Fixed, DiffTime...):

    +--------+--------+--------+=========================================+=======================================+
    |  0xc7  |XXXXXXXX|  0x00  | base10 exponent(MessagePack int format) | coefficient(big endian 256-base limbs |
    +--------+--------+--------+=========================================+=======================================+

    Use a MessagePack implementation supporting ext type to marshall it, result value is coefficient * (10 ^ exponent).

-}

module Z.MessagePack
  -- * MessagePack Class
  ( MessagePack(..), Value(..), defaultSettings, Settings(..), JSON.snakeCase, JSON.trainCase
  , -- * Encode & Decode
    DecodeError
  , readMessagePackFile, writeMessagePackFile
  , decode, decode', decodeChunks, decodeChunks', encode, encodeChunks
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

import Z.MessagePack.Base
import qualified Z.Data.JSON    as JSON
import Z.Data.CBytes            (CBytes)
import Z.IO
import qualified Z.IO.FileSystem as FS

-- | Decode a 'MessagePack' instance from file.
readMessagePackFile :: (HasCallStack, MessagePack a) => CBytes -> IO a
readMessagePackFile p = unwrap . decode' =<< FS.readFile p

-- | Encode a 'MessagePack' instance to file.
writeMessagePackFile :: (HasCallStack, MessagePack a) => CBytes -> a -> IO ()
writeMessagePackFile p x = FS.writeFile p (encode x)
