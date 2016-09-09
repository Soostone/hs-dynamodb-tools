module Aws.Test.DynamoDb.Tools.Capacity
    ( tests
    ) where


-------------------------------------------------------------------------------
import           Aws.DynamoDb.Core
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Capacity
-------------------------------------------------------------------------------


tests :: TestTree
tests = testGroup "Aws.DynamoDb.Tools.Capacity"
  [
    estimateWriteUnitsTests
  ]


-------------------------------------------------------------------------------
estimateWriteUnitsTests :: TestTree
estimateWriteUnitsTests = testGroup "estimateWriteUnits"
  [
    -- some examples from the documentation
    testCase "rounds up for under 1kb" $ do
      estimateWriteUnits (Bytes 512) @?= EstimatedWriteUnits 1
  , testCase "rounds up for over 1kb" $ do
      estimateWriteUnits (Bytes 1500) @?= EstimatedWriteUnits 2
  , testCase "rounds up for over 1kb" $ do
      estimateWriteUnits (Bytes 1500) @?= EstimatedWriteUnits 2
  , testProperty "is never negative" $ \bytes ->
      estimateWriteUnits (Bytes bytes) >= EstimatedWriteUnits 0
  , testProperty "is always >= number of KBs" $ \bytes ->
      estimateWriteUnits (Bytes bytes) >= EstimatedWriteUnits (fromIntegral bytes `div` 1000)
  ]


-------------------------------------------------------------------------------
newtype Bytes = Bytes Int

instance DynSize Bytes where
  dynSize (Bytes b) = b
