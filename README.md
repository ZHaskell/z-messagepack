## Z-MessagePack

[![Hackage](https://img.shields.io/hackage/v/Z-MessagePack.svg?style=flat)](https://hackage.haskell.org/package/Z-MessagePack)
[![Linux Build Status](https://github.com/haskell-Z/z-messagepack/workflows/ubuntu-ci/badge.svg)](https://github.com/haskell-Z/z-messagepack/actions)
[![MacOS Build Status](https://github.com/haskell-Z/z-messagepack/workflows/osx-ci/badge.svg)](https://github.com/haskell-Z/z-messagepack/actions)
[![Windows Build Status](https://github.com/haskell-Z/z-messagepack/workflows/win-ci/badge.svg)](https://github.com/haskell-Z/z-messagepack/actions)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.svg)](https://gitter.im/Z-Haskell/community)

This package is part of [Z](https://github.com/haskell-Z/Z) project, provides MessagePack codecs.

## Requirements

* A working haskell compiler system, GHC(>=8.6), cabal-install(>=2.4), hsc2hs.

* Tests need [hspec-discover](https://hackage.haskell.org/package/hspec-discover).

## Example usage

```haskell
{-# LANGUAGE DeriveGeneric, DeriveAnyClass, DerivingStrategies, TypeApplication #-}

import           GHC.Generics
import qualified Z.MessagePack as MessagePack
import           Z.MessagePack (MessagePack)
import qualified Z.Data.Text as T

data Person = Person
    { name  :: T.Text
    , age   :: Int
    , magic :: Bool
    }
  deriving (Show, Generic)
  deriving anyclass MessagePack

> MessagePack.encode [Person {name = "Erik Weisz", age = 52, magic = True}]
> [145,131,164,110,97,109,101,170,69,114,105,107,32,87,101,105,115,122,163,97,103,101,52,165,109,97,103,105,99,195]
```

