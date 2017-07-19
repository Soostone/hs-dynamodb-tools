{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE TemplateHaskell #-}

module Aws.DynamoDb.Tools.Logger where

-------------------------------------------------------------------------------
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Data.Monoid
import           Katip
import           Katip.Scribes.Handle
import           Language.Haskell.TH
-------------------------------------------------------------------------------


runKatipStdout :: (MonadIO m) =>  KatipContextT m a -> m a
runKatipStdout f = do
  le <- liftIO (ioLogEnv DebugS V3)
  runKatipContextT le () mempty f


-------------------------------------------------------------------------------
-- | For use in logRetries. Retries are Debug, errors are
logRetryCond :: ExpQ
logRetryCond = [| \ res msg ->
  let sev = if res then DebugS else WarningS
  in  $(logTM) sev (ls msg) |]


-------------------------------------------------------------------------------
-- | A NOOP function to work with logRetries.
nologRetry :: Monad m => t -> t1 -> t2 -> m ()
nologRetry _ _ _ = return ()


-------------------------------------------------------------------------------
-- | Log all exceptions.
logFailure :: ExpQ
logFailure = [| $(logFailureWhen) (const True) |]


-------------------------------------------------------------------------------
-- | Provide a function to check whether failure should be logged.
logFailureWhen :: ExpQ
logFailureWhen =
  [| \chk f -> do
       res <- try f
       case res of
         Right a -> return a
         Left e -> do
           when (chk e) $
             $(logTM) ErrorS (ls $ "Exception: " <> show e)
           throwM (e :: SomeException)
  |]
