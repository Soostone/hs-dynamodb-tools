{-# LANGUAGE OverloadedStrings #-}
module Aws.Test.DynamoDb.Tools.TimeSeries.Sparse
    ( tests
    ) where


-------------------------------------------------------------------------------
import           Control.Concurrent.STM.TSem
import           Control.Lens
import           Control.Monad.Trans.Resource
import           Control.Retry
import           Data.Conduit
import qualified Data.Conduit.List                    as CL
import           Data.Time
import           Data.Time.Clock.POSIX
import           Test.Tasty
import           Test.Tasty.HUnit
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Connection
import           Aws.DynamoDb.Tools.TimeSeries.Sparse
import           TestHelpers
-------------------------------------------------------------------------------



tests :: TSem -> TestTree
tests sem = testGroup "Aws.DynamoDb.Tools.TimeSeries.Sparse"
  [
    withDDBState sem $ \mkDDB -> testCase "getCells steams entire table on unqualified getCells in reverse order by default" $ do
      ddb <- mkDDB
      cells <- runDDB ddb $ runResourceT $ do
        saveCell rp Nothing item1
        saveCell rp Nothing item2
        runConduit (getCells rp (_tsSeriesKey item1) Nothing Nothing 1 .| CL.consume)
      cells @?= [Right item2, Right item1]
  ]


-------------------------------------------------------------------------------
item1 :: TSItem
item1 = TSItem (posixSecondsToUTCTime 0) "series key" "item1"


-------------------------------------------------------------------------------
item2 :: TSItem
item2 = item1 & tsTimestamp %~ addUTCTime 30


-------------------------------------------------------------------------------
rp :: RetryPolicy
rp = dynRetryPolicy 5
