module Main
    ( main
    ) where


-------------------------------------------------------------------------------
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TSem
import           Test.Tasty
-------------------------------------------------------------------------------
import qualified Aws.Test.DynamoDb.Tools.TimeSeries.Sparse
import qualified Data.Test.Time.Bins
-------------------------------------------------------------------------------


main :: IO ()
main = do
  sem <- atomically (newTSem 1)
  defaultMain (testSuite sem)



-------------------------------------------------------------------------------
testSuite :: TSem -> TestTree
testSuite sem = testGroup "hs-dynamodb-tools"
  [
    Data.Test.Time.Bins.tests
  , Aws.Test.DynamoDb.Tools.TimeSeries.Sparse.tests sem
  ]
