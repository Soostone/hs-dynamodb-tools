{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}

module Data.Time.Bins where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Lens
import           Control.Monad
import           Data.Aeson.TH
import           Data.Data
import           Data.Int
import           Data.SafeCopy
import           Data.Serialize
import           Data.String
import           Data.Time
import           Data.Time.Calendar.WeekDate
import           Data.Time.Clock.POSIX
import           GHC.Generics
-------------------------------------------------------------------------------



newtype SerializeUtcTime = SerializeUtcTime UTCTime
makePrisms ''SerializeUtcTime

instance Serialize SerializeUtcTime where
    put (SerializeUtcTime t) = do
        put $ toModifiedJulianDay $ utctDay t
        put $ toRational $ utctDayTime t
    get  = new <|> old
        where
          old = SerializeUtcTime . posixSecondsToUTCTime . fromInteger <$> get
          new = liftM SerializeUtcTime $ do
              day <- ModifiedJulianDay <$> get
              diff <- fromRational <$> get
              return $ UTCTime day diff


putUTC :: Getting UTCTime s UTCTime -> s -> Put
putUTC l a = put $ SerializeUtcTime $ a ^. l

getUTC :: AReview r UTCTime -> Get r
getUTC l = (^. (_SerializeUtcTime . re l)) `liftM` get

-------------------------------------------------------------------------------
newtype UtcYear  = UtcYear { _unUtcYear :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcQuarter  = UtcQuarter { _unUtcQuarter :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcMonth  = UtcMonth { _unUtcMonth :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcWeek  = UtcWeek { _unUtcWeek :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcDay  = UtcDay { _unUtcDay :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcHour  = UtcHour { _unUtcHour :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcMinute  = UtcMinute { _unUtcMinute :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)

newtype UtcSecond  = UtcSecond { _unUtcSecond :: UTCTime }
    deriving (Eq,Show,Read,Ord,Typeable)



class TruncatedTime a where
    truncateTime :: UTCTime -> a


-------------------------------------------------------------------------------
instance TruncatedTime UtcYear where truncateTime = UtcYear . truncateUtc BinYear
instance TruncatedTime UtcQuarter where truncateTime = UtcQuarter . truncateUtc BinQuarter
instance TruncatedTime UtcMonth where truncateTime = UtcMonth . truncateUtc BinMonth
instance TruncatedTime UtcWeek where truncateTime = UtcWeek . truncateUtc BinWeek
instance TruncatedTime UtcDay where truncateTime = UtcDay . truncateUtc BinDay
instance TruncatedTime UtcHour where truncateTime = UtcHour . truncateUtc BinHour
instance TruncatedTime UtcMinute where truncateTime = UtcMinute . truncateUtc BinMinute
instance TruncatedTime UtcSecond where truncateTime = UtcSecond . truncateUtc BinSecond
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
data BinSize
    = BinForever
    | BinYear
    | BinQuarter
    | BinMonth
    | BinWeek
    | BinDay
    | BinHour
    | BinMinute
    | BinSecond
  deriving (Eq,Show,Read,Enum,Bounded,Ord,Typeable,Data,Generic)

instance Serialize BinSize


-------------------------------------------------------------------------------
renderBinSize :: IsString a => BinSize -> a
renderBinSize b = case b of
                    BinForever -> "Forever"
                    BinYear -> "Year"
                    BinQuarter -> "Quarter"
                    BinMonth -> "Month"
                    BinWeek -> "Week"
                    BinDay -> "Day"
                    BinHour -> "Hour"
                    BinMinute -> "Minute"
                    BinSecond -> "Second"


-------------------------------------------------------------------------------
truncateUtc :: BinSize -> UTCTime -> UTCTime
truncateUtc BinWeek (UTCTime day _) = UTCTime (fromWeekDate yr wk 1) 0
    where
      (yr,wk,_) = toWeekDate day

truncateUtc bin (UTCTime day diff) = UTCTime (fromGregorian yr' mo' d') diff'
    where
      (yr, mo, d) = toGregorian day

      yr' = if | bin == BinForever -> 2012
               | otherwise -> yr

      mo' = if | bin <= BinYear    -> 1
               | bin == BinQuarter -> truncQuarter mo
               | otherwise -> mo

      d' = if | bin < BinDay   -> 1
              | otherwise      -> d

      diff' = if | bin < BinHour -> 0
                 | bin == BinHour -> truncDiff 3600 diff
                 | bin == BinMinute -> truncDiff 60 diff
                 | bin == BinSecond -> truncDiff 1 diff
                 | otherwise -> diff


truncDiff :: (Integral a, RealFrac b, Fractional b, Num c) => a -> b -> c
truncDiff bin diff = fromIntegral $ floor (diff / fromIntegral bin) * bin

truncQuarter :: Integral a => a -> a
truncQuarter mo = (((mo-1) `div` 3) * 3) + 1


binSeconds :: BinSize -> Int64
binSeconds g = case g of
              BinSecond -> 1
              BinMinute -> 60
              BinHour -> 3600
              BinDay -> 3600 * 24
              BinWeek -> 3600 * 24 * 7
              BinMonth -> 3600 * 24 * 30
              BinQuarter -> 3600 * 24 * 90
              BinYear -> 3600 * 24 * 365
              BinForever -> 3600 * 24 * 365 * 50


-------------------------------------------------------------------------------
deriveSafeCopy 1 'base ''BinSize
deriveJSON defaultOptions ''BinSize
makeLenses ''UtcYear
makeLenses ''UtcQuarter
makeLenses ''UtcMonth
makeLenses ''UtcWeek
makeLenses ''UtcDay
makeLenses ''UtcHour
makeLenses ''UtcMinute
makeLenses ''UtcSecond
-------------------------------------------------------------------------------

instance Serialize UtcYear where
    put = putUTC unUtcYear
    get = getUTC unUtcYear

instance Serialize UtcQuarter where
    put = putUTC unUtcQuarter
    get = getUTC unUtcQuarter

instance Serialize UtcMonth where
    put = putUTC unUtcMonth
    get = getUTC unUtcMonth

instance Serialize UtcWeek where
    put = putUTC unUtcWeek
    get = getUTC unUtcWeek

instance Serialize UtcDay where
    put = putUTC unUtcDay
    get = getUTC unUtcDay

instance Serialize UtcHour where
    put = putUTC unUtcHour
    get = getUTC unUtcHour

instance Serialize UtcMinute where
    put = putUTC unUtcMinute
    get = getUTC unUtcMinute

instance Serialize UtcSecond where
    put = putUTC unUtcSecond
    get = getUTC unUtcSecond


-------------------------------------------------------------------------------
test :: IO ()
test = do
    now <- getCurrentTime
    print now
    mapM_ (\ b -> print (b, truncateUtc b now) ) [minBound..maxBound]
