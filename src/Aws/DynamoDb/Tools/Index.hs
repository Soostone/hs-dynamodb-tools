{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE MultiWayIf                #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TypeFamilies              #-}

-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- When we need to maintain our own arbitrary indexes on DynamoDb.
----------------------------------------------------------------------------


module Aws.DynamoDb.Tools.Index
    ( DynIndex (..)
    , addToIndex
    , listIndex
    , sinkSet
    , deleteIndex
    ) where

-------------------------------------------------------------------------------
import           Aws.Aws
import           Aws.DynamoDb
import           Control.Error
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import           Crypto.Hash
import           Data.Bits
import           Data.Byteable
import qualified Data.ByteString               as B
import           Data.Conduit
import qualified Data.Conduit.List             as C
import           Data.List
import           Data.SafeCopy
import qualified Data.Serialize                as SL
import qualified Data.Set                      as S
import           Data.String.Conv
import           Data.Text                     (Text)
import           Data.Typeable
import           Data.Word
import           Katip
import           MultiCompression
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Connection
import           Aws.DynamoDb.Tools.Types
-------------------------------------------------------------------------------


{- |

Indexes associate a key with a list of values that can be obtained at
a later time. Any key type that implements HasId can be used in an
index.

We use SHA256 to compress keys of arbitrary size, while doing our best
to avoid collisions. In the highly unlikely event, this may cause our
index to "forget" a previously entered value and never return it in
query results.

Furthermore, entries for the same key are sharded across multiple keys
to get DynamoDb to distribute its load across multiple nodes. The
sharding is done based on the value so that inserting the same value
multiple times does not incur redundant entries in the index.

-}


dynIndexTable :: CreateTable
dynIndexTable = CreateTable "index"
  [ AttributeDefinition "k" AttrBinary
  , AttributeDefinition "v" AttrBinary ]
  (HashAndRange "k" "v")
  (ProvisionedThroughput 10 10)
  [] []


-------------------------------------------------------------------------------
-- | Index values have unique key types. We lock down the types to
-- minimize chances of an error.
class (Typeable (DynIndexVal k),  SafeCopy (DynIndexVal k)) => DynIndex k where

    -- | Each key points to a single value type. Same value type may
    -- be used with several different keys.
    type DynIndexVal k

    -- | # of shards for this index. Higher number of shards provides
    -- for concurrency at the expense of overhead. Over time, it is
    -- safe to increase this number but never safe to decrease it.
    indexShards :: k -> Word64

    -- | A stable namespace for values of this key type. Reduces
    -- chances of collisons.
    indexNamespace :: k -> Text

    -- | How to serialize keys in index. Make sure this is very stable
    -- over time; if keys' representation change, we won't be able to find
    -- them in the index later.
    indexSerializeKey :: k -> B.ByteString



-------------------------------------------------------------------------------
-- | Delete all entries in a given index namespace.
-- deleteIndex = undefined
deleteIndex
    :: ( Functor m
       , MonadIO m
       , DdbQuery m
       , MonadMask m
       , Applicative m
       , KatipContext m
       , DynIndex k)
    => RetryPolicy
    -> k
    -> ResourceT m ()
deleteIndex pol k = do
    runConduit (listIndex' pol k .| C.mapM_ del .| C.sinkNull)
  where
    del (v, i) = do
        tbl <- lift $ dynTableFullname dynIndexTable
        void $ cDynN pol $ (deleteItem tbl (mkKey v i))

    mkKey v i = PrimaryKey
      (attr "k" (mkIndexKey k i))
      (Just (attr "v" (DynSc v)))


-------------------------------------------------------------------------------
-- | Add item to index. Adding multiple times is idempotent.
addToIndex
    :: ( Functor m
       , MonadIO m
       , DdbQuery m
       , MonadMask m
       , Katip m
       , Applicative m
       , KatipContext m
       , DynIndex k)
     => k                       -- ^ Index key
     -> DynIndexVal k           -- ^ Index value
     -> ResourceT m ()
addToIndex k v = do
    tbl <- lift $ dynTableFullname dynIndexTable
    let i = toItem $ mkIndexItem k v
        req = (putItem tbl i)
              { piExpect = Conditions CondAnd
                           [Condition "v" (NotEq (toValue (DynSc v))) ]}

    -- exceptions from conditional failures are expected routinely.
    -- catch them locally here.
    recoverDyn (dynRetryPolicy 5) $ (void (cDyn req)) `catch`
      (\e -> case ddbErrCode e of
               ConditionalCheckFailedException -> return ()
               _ -> throwM e)


-------------------------------------------------------------------------------
-- | Stream elements in index
listIndex
    :: (MonadIO m
       , DdbQuery m
       , MonadMask m
       , Katip m
       , Applicative m
       , KatipContext m
       , DynIndex k)
    => RetryPolicy
    -> k
    -> ConduitM () (DynIndexVal k) (ResourceT m) ()
listIndex pol k = listIndex' pol k .| C.map fst


-------------------------------------------------------------------------------
sinkSet :: (Monad m, Ord a) => ConduitM a () m (S.Set a)
sinkSet = C.fold (flip S.insert) S.empty


-------------------------------------------------------------------------------
-- | Stream elements in index with the shard they belong to.
listIndex'
    :: ( MonadIO m
       , DdbQuery m
       , Applicative m
       , KatipContext m
       , DynIndex k)
    => RetryPolicy
    -> k
    -- ^ Index key
    -> ConduitM () (DynIndexVal k, Word64) (ResourceT m) ()
    -- ^ Enumeration of values in index with the shard they are from
listIndex' pol k = do
    tbl <- lift . lift $ dynTableFullname dynIndexTable

    -- query for the Nth shard in ring.
    let q i = (query tbl (Slice (indexKeyShard k i) Nothing))
              { qConsistent = False }

    forM_ [0 .. (indexShards k - 1)] $ \ i ->
          awsIteratedList' (cDynN pol) (q i) .|
          C.mapMaybe (fmap (\ v -> (_unDynSc . indexVal $ v, i)) . hush . fromItem)



-------------------------------------------------------------------------------
-- | Keys for our index system contain SHA256 of namespace, the key
-- unique id and shard number.
mkIndexKey :: DynIndex k => k -> Word64 -> DynHash
mkIndexKey k shard = DynHash . toBytes $ ik'
  where
    ik' :: Digest SHA256
    ik' = hash $ SL.encode (k', indexSerializeKey k, shard)
    k' = toSL (indexNamespace k) :: B.ByteString


-------------------------------------------------------------------------------
-- | Index key of an item for Nth shard
indexKeyShard
    :: (DynIndex k)
    => k
    -> Word64
    -> Attribute
indexKeyShard k i = attr "k" (mkIndexKey k i)


-------------------------------------------------------------------------------
mkIndexItem
    :: DynIndex k
    => k                        -- ^ index key
    -> DynIndexVal k            -- ^ index value (payload)
    -> IndexItem (DynIndexVal k)
mkIndexItem k v = IndexItem (mkIndexKey k shard) (DynSc v)
    where
      h = digestInt $ hash (v ^. re (safeCopyCompress Gzip))
      shard = h `mod` indexShards k


-------------------------------------------------------------------------------
-- | Extract the first 64 bits out of an MD5 digest. Used for
-- determining data shard, so we don't care much about loss of
-- precision.
digestInt :: Digest MD5 -> Word64
digestInt = foldl' step 0 . take 8 . B.unpack . toBytes
    where
      step cur a = shift cur 8 .|. fromIntegral a



-- | An index from keys of type k to values of type v. This type is
-- not used directly by the user.
--
-- In general, one would expect that for each k, there are numerous
-- values of v. This index then allows for query-scanning across the
-- values for a given key.
--
-- Example: We can have:
--    key --> (Campaign 12, BinHourly)
--    value --> timeseries-key-for-such-and-such-hour
data IndexItem v = IndexItem {
      indexKey :: DynHash
      -- ^ A unique key identifying this index; make sure it does not
      -- have overlaps with other stuff.
    , indexVal :: DynSc v
    -- ^ Index to this value (the range key on DDB)
    }



instance (SafeCopy v) => ToDynItem (IndexItem v) where
    toItem IndexItem{..} = item
      [ attr "k" indexKey
      , attr "v" indexVal ]


instance (Typeable v, SafeCopy v) => FromDynItem (IndexItem v) where
    parseItem m = IndexItem
      <$> getAttr "k" m
      <*> getAttr "v" m
