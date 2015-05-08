{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE TemplateHaskell #-}

module Aws.DynamoDb.Tools.Logger where

-------------------------------------------------------------------------------
import           Control.Monad
import           Control.Monad.Catch
import           Data.Monoid
import           Katip
import           Katip.Scribes.Handle
import           Language.Haskell.TH
-------------------------------------------------------------------------------


runKatipStdout :: BlankLogT (KatipT m) a -> m a
runKatipStdout = runKatipT _ioLogEnv . runBlankLogT


-------------------------------------------------------------------------------
-- | For use in logRetries. Retries are Debug, errors are
logRetryCond :: ExpQ
logRetryCond = [| \ res msg ->
  let sev = if res then DebugS else WarningS
  in  $(logTM) sev (ls msg) |]


-------------------------------------------------------------------------------
-- | A NOOP function to work with logRetries.
nologRetry :: Monad m => t -> t1 -> m ()
nologRetry _ _ = return ()


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
