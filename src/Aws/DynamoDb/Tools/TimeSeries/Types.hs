{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module Aws.DynamoDb.Tools.TimeSeries.Types where

-------------------------------------------------------------------------------
import           Aws.DynamoDb
import           Control.Lens
import qualified Data.ByteString.Char8    as B
import           Data.Time
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Types
-------------------------------------------------------------------------------


dynTimeseriesTable :: CreateTable
dynTimeseriesTable = CreateTable "timeseries"
    [ AttributeDefinition "_k" AttrBinary
    , AttributeDefinition "_t" AttrNumber]
    (HashAndRange "_k" "_t")
    (ProvisionedThroughput 10 10)
    [] []



-------------------------------------------------------------------------------
-- | Every item stored as a sparse series must be able to point at the
-- precise point in time to which it corresponds.
class TimeCursor a where
    timeCursor :: a -> UTCTime


-------------------------------------------------------------------------------
-- | These objects can tell when they were last updatd. Law: Every
-- update must change this value, so that it is impossible for an
-- object to receive a mutation without altering this information.
class UpdateCursor a where
    updatedAt :: a -> UTCTime
    updateCursor :: Lens' a DynUuid


-------------------------------------------------------------------------------
-- | Every item stored as a sparse series must have a uniquely
-- identifying key. If storing multiple types of objects in the same
-- table, you need to make sure there is no overlap between the
-- different types. Easily achieved by prefixing a namespace.
class (ToDynItem a, FromDynItem a, TimeCursor a, UpdateCursor a
      , SeriesKey (TimeSeriesKey a)) => TimeSeries a where

    type TimeSeriesKey a

    getSeriesKey :: a -> TimeSeriesKey a


-------------------------------------------------------------------------------
-- | Types that can be used as keys in timeseries
class SeriesKey k where
    serializeSeriesKey :: k -> B.ByteString

instance SeriesKey () where serializeSeriesKey _ = ""
instance SeriesKey B.ByteString where serializeSeriesKey k = k


-------------------------------------------------------------------------------
seriesKey :: TimeSeries a => a -> B.ByteString
seriesKey = serializeSeriesKey . getSeriesKey


-------------------------------------------------------------------------------
seriesKeyAttr :: SeriesKey k => k -> Attribute
seriesKeyAttr k = attr "_k" (serializeSeriesKey k)
