{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE ConstraintKinds           #-}
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
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE TypeFamilies              #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Aws.DynamoDb.Tools.TimeSeries.Binned
-- Copyright   :  Soostone Inc
-- License     :  All Rights Reserved
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
--
----------------------------------------------------------------------------

module Aws.DynamoDb.Tools.TimeSeries.Binned where

-------------------------------------------------------------------------------
import           Aws.DynamoDb
import           Control.Applicative
import           Control.Error
import           Control.Lens
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.Trans.Resource
import           Data.Bifunctor
import qualified Data.ByteString.Base64               as Base64
import           Data.ByteString.Char8                (ByteString)
import           Data.Default
import           Data.Foldable                        (Foldable)
import qualified Data.Map                             as M
import           Data.SafeCopy
import           Data.Semigroup                       as Semigroup
import           Data.String.Conv
import           Data.Time
import           Data.Time.Bins
import           Data.Time.Locale.Compat              as LC
import           Data.Typeable
import           Data.UUID
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Connection
import           Aws.DynamoDb.Tools.TimeSeries.Sparse
import           Aws.DynamoDb.Tools.TimeSeries.Types
import           Aws.DynamoDb.Tools.Types
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
showTime :: FormatTime t => t -> String
showTime d = formatTime LC.defaultTimeLocale "%m/%d/%Y %H:%M:%S" d


-------------------------------------------------------------------------------
timedCSV :: SeriesKey k => Timed k a -> [(ByteString, ByteString)]
timedCSV Timed{..} =
    [ ("time_bin", toS $ show _timedBin)
    , ("time_at", toS $ showTime _timedAt)
    , ("time_updated", toS $ showTime _timedUpdated)
    , ("time_cursor", toASCIIBytes . _getDynUuid $ _timedCursor)
    , ("time_key", Base64.encode $ serializeSeriesKey _timedKey)
    ]


-------------------------------------------------------------------------------
-- | A time-tagged container for values. Properties are:
--
-- 1. It contains the time aspects, so the values do not need to.
-- Therefore, any kind of data can really be put in a time series.
--
-- 2. It bins the timestamps, which is essentially meant for an
-- aggregation scenario.
data Timed k a = Timed {
      _timedBin     :: !BinSize
    , _timedAt      :: !UTCTime
    -- ^ Bin's starting point
    , _timedUpdated :: !UTCTime
    -- ^ Update cursor of the last merged in data, i.e. the timestamp
    -- of the last input processed into this timed value.
    , _timedCursor  :: !DynUuid
    -- ^ Unique token generated for the last time this object was
    -- saved.
    , _timedKey     :: !k        -- ^ Key for this series
    , _timedVal     :: !a
    }
makeLenses ''Timed
deriveSafeCopy 1 'base ''Timed


instance Semigroup.Semigroup a => Semigroup (Timed k a) where
    t1 <> t2 = t2
      & combine timedUpdated max t1 t2
      & combine timedCursor (\ _ b -> b) t1 t2
      & combine timedVal (<>) t1 t2


instance Functor (Timed k) where
    fmap f t = t & timedVal %~ f


-------------------------------------------------------------------------------
dummyTimed :: Timed () ()
dummyTimed = Timed BinDay t t nilUuid () ()
  where
    t = UTCTime (fromGregorian 2000 1 1) 0


-------------------------------------------------------------------------------
-- | Combine binned cells over their value
mergeTimedWith :: (a -> a -> a) -> Timed k a -> Timed k a -> Timed k a
mergeTimedWith f t1 t2 = t2
      & combine timedUpdated max t1 t2
      & combine timedCursor (\ _ b -> b) t1 t2
      & combine timedVal f t1 t2


instance (FromDynItem a, SafeCopy k, Typeable k) => FromDynItem (Timed k a) where
    parseItem m = Timed
      <$> (_unDynSc <$> getAttr "_bs" m)
      <*> getAttr "_t" m
      <*> getAttr "_l" m
      <*> (getAttr "_s" m <|> pure nilUuid)
      <*> (_unDynSc <$> getAttr "k" m)
      <*> parseItem m


instance (ToDynItem a, SafeCopy k) => ToDynItem (Timed k a) where
    toItem Timed{..} = item
      [ attr "_bs" (DynSc _timedBin)
      , attr "_t" _timedAt
      , attr "_l" _timedUpdated
      , attr "_s" _timedCursor
      , attr "k" (DynSc _timedKey) ]
      `M.union` toItem _timedVal


-------------------------------------------------------------------------------
-- | Key type for binned timeseries. A series key bundled with the bin size.
data TimeKey k = TimeKey {
      _timeKeySize   :: BinSize
    , _timeKeySeries :: k
    } deriving (Eq,Show,Read,Ord,Typeable)
makeLenses ''TimeKey


instance SeriesKey k => SeriesKey (TimeKey k) where
    serializeSeriesKey TimeKey{..} =
      (serializeSeriesKey _timeKeySeries, _timeKeySize) ^. re pSerialize


instance TimeCursor (Timed k a) where
    timeCursor = _timedAt


instance UpdateCursor (Timed k a) where
    updatedAt = _timedUpdated
    updateCursor = timedCursor


-------------------------------------------------------------------------------
-- | Timed cells get an automatic TimeSeries instance.
instance (ToDynItem a, FromDynItem a, SeriesKey k, SafeCopy k, Typeable k) =>
  TimeSeries (Timed k a) where

    type TimeSeriesKey (Timed k a) = TimeKey k
    getSeriesKey Timed{..} = TimeKey _timedBin _timedKey


-------------------------------------------------------------------------------
makeBins :: [BinSize] -> (k, a, UTCTime) -> [Timed k a]
makeBins bss (k, a, now) = flip map bss $ \ bs ->
    let bin = truncateUtc bs now
    in Timed bs bin now nilUuid k a


-------------------------------------------------------------------------------
-- | A dictionary-based representation of timeseries data. Useful for
-- deduplication, streaming merging and lookups.
newtype TimedMap k a = TimedMap {
      getTimedMap :: M.Map (BinSize, k, UTCTime) (Timed k a)
    }


-------------------------------------------------------------------------------
instance Ord k => Default (TimedMap k a) where
    def = TimedMap mempty


-------------------------------------------------------------------------------
insertTimed :: Ord k => (a -> a -> a) -> Timed k a -> TimedMap k a -> TimedMap k a
insertTimed f !a !m = TimedMap $! M.insertWith f' key a (getTimedMap m)
    where
      f' = mergeTimedWith f
      key = (a ^. timedBin, a ^. timedKey, a ^. timedAt)


-------------------------------------------------------------------------------
extractCells :: TimedMap k a -> [Timed k a]
extractCells = M.elems . getTimedMap


-------------------------------------------------------------------------------
-- | Figure out items that belong in the same bin and collapse using
-- the given collapse function. Used when processing lots of items in
-- batch.
collapseSeries
    :: (Ord k, Foldable f)
    => (a -> a -> a)
    -> f (Timed k a)
    -> [Timed k a]
collapseSeries f ts = M.elems $ collect mkKey id comb ts
    where
      mkKey t = (t ^. timedBin, t ^. timedKey, t ^. timedAt)
      comb = mergeTimedWith f


-------------------------------------------------------------------------------
mergeIntoStream
    :: ( DdbQuery n
       , MonadUnliftIO n
       , ToDynItem a, FromDynItem a, SafeCopy k, SeriesKey k, Typeable k)
    => RetryPolicy
    -> (Maybe (Timed k a) -> (Timed k a) -> Maybe (Timed k a))
    -- ^ how to update with values in the database
    -> Timed k a
    -> n (Either String ())
mergeIntoStream pol f t = mergeOne t
  where

    mergeOne = runResourceT .
      liftM (first (show :: SomeException -> String)) . try .
      recoverConditionalCheck 6 . mergeOne'

    mergeOne' x = do
        old <- getCell pol (getSeriesKey x) (x ^. timedAt) <&> hush
        case f old x of
          Nothing -> return ()
          Just a  -> saveCell pol old a



