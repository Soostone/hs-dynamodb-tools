{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}
module TestHelpers where


-------------------------------------------------------------------------------
import qualified Aws                                 as Aws
import           Aws.DynamoDb
import           Control.Applicative                 as A
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TSem
import           Control.Error
import           Control.Lens
import           Control.Monad.Base
import           Control.Monad.Catch
import           Control.Monad.IO.Unlift
import           Control.Monad.Reader
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Resource
import           Control.Retry
import           Data.Aeson
import           Data.ByteString                     (ByteString)
import           Data.Semigroup                      as Semigroup
import           Data.Text                           (Text)
import qualified Data.Text                           as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Yaml
import           Katip
import           Network.HTTP.Conduit
import           System.Environment
import           System.IO
import           Test.Tasty
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Connection
import           Aws.DynamoDb.Tools.Table
import           Aws.DynamoDb.Tools.TimeSeries.Types
import           Aws.DynamoDb.Tools.Types
-------------------------------------------------------------------------------


nullTime :: UTCTime
nullTime = posixSecondsToUTCTime 0


-------------------------------------------------------------------------------
data TSItem = TSItem {
      _tsTimestamp :: !UTCTime
    , _tsSeriesKey :: !ByteString
    , _tsPayload   :: !Text
    } deriving (Show, Eq)


makeLenses ''TSItem


instance TimeCursor TSItem where
  timeCursor = _tsTimestamp


instance UpdateCursor TSItem where
  updatedAt = const nullTime
  updateCursor = lens (const nilUuid) (\x _ -> x)


instance TimeSeries TSItem where
  type TimeSeriesKey TSItem = ByteString
  getSeriesKey = _tsSeriesKey


instance ToDynItem TSItem where
  toItem TSItem {..} = item [ attr "v" _tsPayload
                            , attr "_k" _tsSeriesKey
                            , attr "_t" _tsTimestamp
                            ]

instance FromDynItem TSItem where
  parseItem v = do
    t <- getAttr "_t" v
    k <- getAttr "_k" v
    p <- getAttr "v" v
    return (TSItem t k p)


-------------------------------------------------------------------------------
data DDBState = DDBState {
      _ddbManager   :: !Manager
    , _ddbAwsConfig :: !Aws.Configuration
    , _ddbDConfig   :: !(DdbConfiguration Aws.NormalQuery)
    , _ddbDynConfig :: !DynConfig
    , _ddbLogEnv    :: !LogEnv
    }


makeLenses ''DDBState


-------------------------------------------------------------------------------
newtype DDB m a = DDB {
      unDDB :: ReaderT DDBState m a
    } deriving ( MonadIO
               , Monad
               , Functor
               , A.Applicative
               , MonadCatch
               , MonadThrow
               , MonadMask
               , MonadTrans
               , MonadReader DDBState
               )


instance (MonadUnliftIO m) => MonadUnliftIO (DDB m) where
  askUnliftIO = DDB (go <$> askUnliftIO)
    where
      go :: UnliftIO (ReaderT DDBState m) -> UnliftIO (DDB m)
      go (UnliftIO x) = (UnliftIO (\(DDB r) -> x r))


-------------------------------------------------------------------------------
instance MonadTransControl DDB where
    type StT DDB a = StT (ReaderT DDBState) a
    liftWith f = DDB $ ReaderT $ \r -> f $ \c -> runReaderT (unDDB c) r
    restoreT = DDB . ReaderT . const


-------------------------------------------------------------------------------
instance MonadBase b m => MonadBase b (DDB m) where
    liftBase = liftBaseDefault


-------------------------------------------------------------------------------
instance MonadBaseControl b m => MonadBaseControl b (DDB m) where
   type StM (DDB m) a = ComposeSt DDB m a
   liftBaseWith = defaultLiftBaseWith
   restoreM     = defaultRestoreM


-------------------------------------------------------------------------------
instance (MonadIO m) => Katip (DDB m) where
  getLogEnv = view ddbLogEnv
  localLogEnv f (DDB m) = DDB (local (\ddbState -> ddbState { _ddbLogEnv = f (_ddbLogEnv ddbState)}) m)


-------------------------------------------------------------------------------
-- | We don't really care to use the context or namespace just for these tests
instance (MonadIO m) => KatipContext (DDB m) where
  getKatipNamespace = return mempty
  localKatipNamespace _ m = m
  getKatipContext = return mempty
  localKatipContext _ m = m


-------------------------------------------------------------------------------
instance ( Monad m
         , MonadIO m
         , MonadBaseControl IO m
         , MonadThrow m
         , MonadCatch m
         , MonadMask m) => DdbQuery (DDB m) where
  getDdbManager = view ddbManager
  getAwsConfig = view ddbAwsConfig
  getDdbConfig = view ddbDConfig
  getDynConfig = view ddbDynConfig

-------------------------------------------------------------------------------
withDDBState :: TSem -> (IO DDBState -> TestTree) -> TestTree
withDDBState sem = withResource alloc free
  where alloc = do atomically (waitTSem sem)
                   s <- mkDDBState
                   runDDB s $ do
                     $(logTM) InfoS "Creating table"
                     createTableIfMissing tbl
                     $(logTM) InfoS "Waiting for table creation"
                     created <- waitForTableCreation (constantDelay (3000000) Semigroup.<> limitRetries 10)
                     unless created $ error "Table never got created"
                   return s
        free = resetDDBState


-------------------------------------------------------------------------------
mkDDBState :: IO DDBState
mkDDBState = do
  mgr <- newManager tlsManagerSettings
  cFile <- Aws.credentialsDefaultFile
  profile <- maybe Aws.credentialsDefaultKey T.pack <$> lookupEnv "AWS_PROFILE"
  Just awsCreds <- case cFile of
    Just f  -> Aws.loadCredentialsFromFile f profile
    Nothing -> Aws.loadCredentialsFromEnv
  let noProxy = Nothing
  let awsConf = Aws.Configuration Aws.Timestamp
                                  awsCreds
                                  (Aws.defaultLog Aws.Warning)
                                  noProxy
  TestConfig {..} <- either throwM return =<< decodeFileEither "test/config.yml"
  scr <- mkHandleScribe ColorIfTerminal stderr DebugS V3
  le <- registerScribe "stderr" scr defaultScribeSettings =<< initLogEnv "hs-dynamodb-tools" "test"
  return (DDBState mgr awsConf tcDdbConfiguration tcDynConfig le)


-------------------------------------------------------------------------------
data TestConfig = TestConfig {
      tcDdbConfiguration :: !(DdbConfiguration Aws.NormalQuery)
    , tcDynConfig        :: !DynConfig
    }


instance FromJSON TestConfig where
  parseJSON v = do
    dc <- parseJSON v
    let ddbCfg = fromDynConfig dc
    return (TestConfig ddbCfg dc)
    where fromDynConfig DynConfig {..} =
            DdbConfiguration (Region _dcEndpoint _dcName)
                             _dcProto
                             _dcPort



-------------------------------------------------------------------------------
resetDDBState :: DDBState -> IO ()
resetDDBState ds = runDDB ds $ do
  tblName <- getTblName
  void $ runResourceT (cDynN (dynRetryPolicy 5) (DeleteTable tblName))


-------------------------------------------------------------------------------
runDDB :: DDBState -> DDB m a -> m a
runDDB s f = runReaderT (unDDB f) s


-------------------------------------------------------------------------------
tbl :: CreateTable
tbl = CreateTable "timeseries"
      [ AttributeDefinition "_k" AttrBinary
      , AttributeDefinition "_t" AttrNumber]
      (HashAndRange "_k" "_t")
      (ProvisionedThroughput 5 5)
      [] []


-------------------------------------------------------------------------------
getTblName :: (Functor m, DdbQuery m) => m Text
getTblName = dynTableFullname tbl


-------------------------------------------------------------------------------
waitForTableCreation
    :: forall m. ( MonadIO m
       , MonadUnliftIO m
       , DdbQuery m)
    => RetryPolicy
    -> m Bool
waitForTableCreation rp = do
  nm <- getTblName
  fmap isRight . retrying rp checkAgain $ const $
    try $ runResourceT $ cDyn (DescribeTable nm)
  where checkAgain :: a -> Either DdbError DescribeTableResult -> m Bool
        checkAgain _ (Right r) = return (rTableStatus (dtStatus r) == "CREATING")
        checkAgain _ (Left _) = return True
