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


This property makes it suitable for passing data across language boundary, e.g. from a static typed language to a dynamic one,
at the cost of a lower space efficiency(i.e. type tag and field label).

-}

module Z.Data.MessagePack
  ( -- * MessagePack Class
    MessagePack(..), Value(..), defaultSettings, Settings(..), JSON.snakeCase, JSON.trainCase
    -- * Encode & Decode
  , readMessagePackFile, writeMessagePackFile
  , decode, decode', decodeChunks, decodeChunks', encode, encodeChunks
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

import Z.Data.MessagePack.Base
import qualified Z.Data.JSON    as JSON
import Z.Data.CBytes            (CBytes)
import Z.IO
import qualified Z.IO.FileSystem as FS

-- | Decode a 'MessagePack' instance from file.
readMessagePackFile :: (HasCallStack, MessagePack a) => CBytes -> IO a
readMessagePackFile p = unwrap "EPARSE" . decode' =<< FS.readFile p

-- | Encode a 'MessagePack' instance to file.
writeMessagePackFile :: (HasCallStack, MessagePack a) => CBytes -> a -> IO ()
writeMessagePackFile p x = FS.writeFile p (encode x)
