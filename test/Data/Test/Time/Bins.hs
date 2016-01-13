module Data.Test.Time.Bins
    ( tests
    ) where


-------------------------------------------------------------------------------
import           Data.Serialize
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck
-------------------------------------------------------------------------------
import           Data.Time.Bins
-------------------------------------------------------------------------------


tests :: TestTree
tests = testGroup "Data.Time.Bins"
  [
    testProperty "SerializeUtcTime put/get roundtrips" $ \t ->
      let st = SerializeUtcTime  t
      in runGet get (runPut (put st)) === Right st
  ]
