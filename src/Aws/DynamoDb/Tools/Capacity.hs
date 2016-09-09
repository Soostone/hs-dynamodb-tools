{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Aws.DynamoDb.Tools.Capacity
    ( EstimatedWriteUnits(..)
    , estimateWriteUnits
    ) where


-------------------------------------------------------------------------------
import           Aws.DynamoDb.Core
import           Data.Ratio
import           GHC.Int
-------------------------------------------------------------------------------


-- | Estimated write capacity units that an item will use.
newtype EstimatedWriteUnits = EstimatedWriteUnits {
      estimatedWriteUnits :: Int64
    } deriving (Show, Eq, Num, Ord, Enum, Real, Integral)


-------------------------------------------------------------------------------
estimateWriteUnits :: (DynSize a) => a -> EstimatedWriteUnits
estimateWriteUnits a = EstimatedWriteUnits (fromIntegral kbs)
  where
    bytes = dynSize a
    -- round up to the nearest 1kb
    kbs = ceiling (bytes % 1000)
