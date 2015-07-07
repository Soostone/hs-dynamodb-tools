{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE TypeFamilies              #-}

-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :  Soostone Inc
-- License     :  All Rights Reserved
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- DynamoDb Table operations
----------------------------------------------------------------------------

module Aws.DynamoDb.Tools.Table where

-------------------------------------------------------------------------------
import           Aws.DynamoDb
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import           Katip
-------------------------------------------------------------------------------
import           Aws.DynamoDb.Tools.Connection
import           Aws.DynamoDb.Tools.Types
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Create given table with some default settings.
createTableIfMissing
    :: ( MonadIO m
       , MonadBaseControl IO m
       , DdbQuery m
       , KatipContext m)
    => CreateTable
    -> m ()
createTableIfMissing tbl = do
    nm <- dynTableFullname tbl
    res <- try $ runResourceT $ cDynN (dynRetryPolicy 5) $ DescribeTable nm
    case res of
      Right a -> $(logTM) DebugS $ showLS a
      Left (_ :: DdbError) -> void $ runResourceT $ do
        res <- cDynN (dynRetryPolicy 5) tbl { createTableName = nm }
        $(logTM) DebugS $ showLS res
