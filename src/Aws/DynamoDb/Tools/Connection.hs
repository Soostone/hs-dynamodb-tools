{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Layer for issuing queries to DynamoDb.
----------------------------------------------------------------------------

module Aws.DynamoDb.Tools.Connection where

-------------------------------------------------------------------------------
import qualified Aws                          as Aws
import           Aws.DynamoDb
import           AWS.Utils.Retry              (httpRetryH, networkRetryH)
import           Control.Applicative
import           Control.Error
import           Control.Monad.Catch
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import           Control.Retry
import           Data.Monoid
import           Katip
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Logger
import           Aws.DynamoDb.Tools.Types
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Recover from Aws errors, log ultimate failures.
recoverAws
    :: ( MonadIO m
       , MonadMask m
       , Applicative m
       , KatipContext m)
    => Int
    -> m f
    -> m f
recoverAws n f = $(logFailureWhen) logchk $ recovering
  (awsPolicy n) [dynRetryH, httpRetryH, networkRetryH] f
  where
    logchk e = fromMaybe True $ chk <$> fromException e
    chk e = case ddbErrCode e of
      ConditionalCheckFailedException -> False
      _ -> True



-------------------------------------------------------------------------------
cDynN
    :: ( MonadMask n
       , MonadIO n
       , DdbQuery n
       , Aws.Transaction r b
       , Aws.ServiceConfiguration r ~ DdbConfiguration
       , Applicative n
       , KatipContext n)
    => Int
    -- ^ Number of retries
    -> r
    -> ResourceT n b
cDynN n r = recoverAws n (cDyn r)


-------------------------------------------------------------------------------
-- | Run a DynamoDb query
cDyn
    :: ( MonadMask n
       , MonadIO n
       , DdbQuery n
       , Aws.Transaction r b
       , Aws.ServiceConfiguration r ~ DdbConfiguration
       , KatipContext n
       , Applicative n)
    => r
    -> ResourceT n b
cDyn r = do
    mgr <- lift getDdbManager
    conf <- lift getAwsConfig
    dynConf <- lift getDdbConfig
    hoist liftIO $ Aws.pureAws conf dynConf mgr r


-------------------------------------------------------------------------------
-- | Recover from DynamoDb's 'ValidationException'. Doesn't do any
-- logging as several exceptions are expected to pass through this
-- net.
recoverConditionalCheck
    :: ( MonadIO m
       , Applicative m
       , KatipContext m
       , MonadMask m)
    => Int
    -> m a
    -> m a
recoverConditionalCheck n f = recovering (awsPolicy n) [h] f
  where
    h _ = Handler chk

    chk e = return $ case ddbErrCode e of
        ConditionalCheckFailedException -> True
        _ -> False


-------------------------------------------------------------------------------
-- | Our default policy for AWS ops
--
-- > map (getRetryPolicy (awsPolicy 10)) [0..5]
-- [Just 25000,Just 50000,Just 100000,Just 200000,Just 400000,Just 800000]
awsPolicy :: Int -> RetryPolicy
awsPolicy n = mempty <> limitRetries n <> exponentialBackoff 25000


-------------------------------------------------------------------------------
-- | Catch dyndb errors that make sense
dynRetryH :: (MonadIO m, Applicative m, KatipContext m) => Int -> Handler m Bool
dynRetryH n = logRetries
  (return . shouldRetry . ddbErrCode)
  nologRetry
  n



