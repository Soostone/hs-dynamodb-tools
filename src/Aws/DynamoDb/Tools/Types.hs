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
{-# OPTIONS -Wall               #-}

-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Base types and utilities.
----------------------------------------------------------------------------

module Aws.DynamoDb.Tools.Types where

-------------------------------------------------------------------------------
import qualified Aws                         as Aws
import           Aws.Core
import           Aws.DynamoDb
import           Control.Applicative
import           Control.Error
import           Control.Lens                hiding (au)
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Monad.Trans.Control
import           Crypto.Hash.SHA256          as Crypto (hash)
import qualified Data.ByteString.Char8       as B
import qualified Data.Foldable               as F
import           Data.Hashable
import qualified Data.Map                    as M
import           Data.Monoid
import qualified Data.SafeCopy               as SC
import qualified Data.Serialize              as SL
import           Data.String
import           Data.String.Conv
import           Data.Text                   (Text)
import qualified Data.Text                   as T
import qualified Data.Text.Encoding          as T
import           Data.Typeable
import           Data.UUID
import           Data.Yaml                   ((.:), (.:?))
import qualified Data.Yaml                   as Y
import           GHC.Generics
import           Katip                       hiding (Item)
import           MultiCompression
import           Network.HTTP.Conduit
import           System.Random
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
class (MonadIO m, Functor m, KatipContext m, Applicative m,
       MonadMask m, MonadBaseControl IO m) =>
    DdbQuery m where
    getDdbManager :: m Manager
    getAwsConfig :: m Aws.Configuration
    getDdbConfig :: m (DdbConfiguration Aws.NormalQuery)
    getDynConfig :: m DynConfig



-------------------------------------------------------------------------------
data DynConfig = DynConfig {
      _dcNamespace :: Text
    , _dcEndpoint  :: B.ByteString
    , _dcName      :: B.ByteString
    , _dcProto     :: Protocol
    , _dcPort      :: Maybe Int
    } deriving (Eq,Show)
makeClassy ''DynConfig



-------------------------------------------------------------------------------
-- | Grab table name
dynTableFullname
    :: (Functor m, DdbQuery m)
    => CreateTable
    -> m Text
dynTableFullname t = withNS (createTableName t) . _dcNamespace <$> getDynConfig


-------------------------------------------------------------------------------
withNS :: (IsString m, Monoid m) => m -> m -> m
withNS tbl ns = ns <> "_" <> tbl


-------------------------------------------------------------------------------
schemaKeys :: KeySchema -> [Text]
schemaKeys (HashOnly k) = [k]
schemaKeys (HashAndRange k v) = [k, v]


instance Y.FromJSON DynConfig where
    parseJSON (Y.Object v) = DynConfig
      <$> v .: "namespace"
      <*> (T.encodeUtf8 <$> v .: "endpoint")
      <*> (T.encodeUtf8 <$> v .: "name")
      <*> (maybe (fail "bad dyn proto") return . readMay =<< v .: "proto")
      <*> v .:? "port"
    parseJSON _ = error "DynConfig must be an Object"


ddbConfig :: HasDynConfig c => c -> DdbConfiguration qt
ddbConfig conf = DdbConfiguration (Region (f dcEndpoint) (f dcName)) (f dcProto) (f dcPort)
    where
      f :: Lens' DynConfig a -> a
      f g = conf ^. (dynConfig.g)


-------------------------------------------------------------------------------
-- | SafeCopy wrapper for gzipped DynamoDb serialization
newtype DynSc a = DynSc { _unDynSc :: a }
    deriving (Eq,Show,Read,Ord,Typeable,Generic)


instance SC.SafeCopy a => DynVal (DynSc a) where
    type DynRep (DynSc a) = DynBinary
    toRep (DynSc a) = DynBinary (a ^. (re (safeCopyCompress Gzip)))
    fromRep dv = fmap DynSc . (^? safeCopyCompress Gzip) =<< fromRep dv


-------------------------------------------------------------------------------
-- | SafeCopy wrapper for gzipped DynamoDb serialization
newtype DynSer a = DynSer { unDynSer :: a }
    deriving (Eq,Show,Read,Ord,Typeable,Generic)


-------------------------------------------------------------------------------
instance SL.Serialize a => DynVal (DynSer a) where
    type DynRep (DynSer a) = DynBinary
    toRep (DynSer a) = DynBinary . (^. re (pCompress Gzip)) . SL.encode $ a
    fromRep dv =
        hush . fmap DynSer . SL.decode =<<
        (^? pCompress Gzip) =<<
        fromRep dv



-------------------------------------------------------------------------------
pico :: Integer
pico = 10 ^ (12 :: Integer)


-------------------------------------------------------------------------------
dayPico :: Integer
dayPico = 86400 * pico




-------------------------------------------------------------------------------
newtype DynHash = DynHash { _dynHash :: B.ByteString }
  deriving (Eq,Show,Read,Ord,SL.Serialize,Typeable,Hashable)

instance SC.SafeCopy DynHash

instance DynVal DynHash where
    type DynRep DynHash = DynBinary
    toRep (DynHash i) = toRep i
    fromRep i = DynHash <$> fromRep i


-------------------------------------------------------------------------------
-- | Convert object to its bytestring rep, then hash to obtain fingerprint.
mkSig :: Prism' B.ByteString a -> a -> DynHash
mkSig p a = DynHash $ Crypto.hash (a ^. re p)


-------------------------------------------------------------------------------
nonempty :: [t] -> Maybe [t]
nonempty [] = Nothing
nonempty xs = Just xs


-------------------------------------------------------------------------------
newtype DynUuid = DynUuid { _getDynUuid :: UUID }
  deriving (Eq,Show,Read,Ord,Random,Typeable)


instance DynVal DynUuid where
    type DynRep DynUuid = DynBinary
    toRep (DynUuid u) = DynBinary . toS . toByteString $ u
    fromRep (DynBinary bs) = DynUuid `fmap` fromByteString (toS bs)


instance SC.SafeCopy DynUuid where
    putCopy (DynUuid u) = SC.contain $ SC.safePut (toByteString u)
    getCopy = SC.contain $ do
      s <- SC.safeGet
      case fromByteString s of
        Nothing -> fail "Can't parse UUID"
        Just x -> return $ DynUuid x


-------------------------------------------------------------------------------
-- | A nil UUID of all zeros. Can be used as a default value during
-- data migrations.
nilUuid :: DynUuid
nilUuid = DynUuid nil



-------------------------------------------------------------------------------
-- | Only keep those attributes from the first item that are not
-- present with the exact same values in the second item.
--
-- If writing to or updating an already existing item, there is an
-- opportunity to send only the updated variables. This function will
-- intelligently confirm that the items would be written to the same
-- cell based on the keys and if so, diff the new and old items.
diffItem
    :: [T.Text]
    -- ^ Hash and range keys that must remain untouched
    -> Item
    -- ^ New item
    -> Item
    -- ^ Old item
    -> Either Item (PrimaryKey, [AttributeUpdate])
diffItem ks new old =
    case sameItem of
        Just pk -> Right (pk, update)
        Nothing -> Left new

    where

      update = itemUpdate (M.differenceWithKey f new old)

      sameItem = do
          ns <- mapM (flip M.lookup new) ks
          os <- mapM (flip M.lookup old) ks
          guard $ ns == os
          return $ mkPrimary (zipWith Attribute ks ns)

      f k n o
          | k `elem` ks = Nothing
          | n == o      = Nothing
          | True        = Just n


      mkPrimary [a,b] = PrimaryKey a (Just b)
      mkPrimary [a] = PrimaryKey a Nothing
      mkPrimary _ = error "unexpected argument to mkPrimary"


itemUpdate :: Item -> [AttributeUpdate]
itemUpdate m = map (au . uncurry Attribute) $ M.toList m



-------------------------------------------------------------------------------
-- | Break a list out into individual keys
breakItem
    :: (DynVal v)
    => T.Text
    -- ^ A prefix for items in this collection
    -> (k -> Text)
    -- ^ A name for each item in collection
    -> (k, v)
    -- ^ Item key-value pair
    -> Attribute
breakItem prefix keyPrint (k,i) = attr k' i
    where
      k' = prefix <> "_" <> keyPrint k


-------------------------------------------------------------------------------
-- | Bring items back together
assembleItem
    :: DynVal v
    => Text
    -- ^ A prefix for items in this collection
    -> (Text -> Maybe k)
    -- ^ How to turn the name portion into key for an item
    -> M.Map Text DValue
    -- ^ The Item from DynamoDb
    -> [(k, v)]
assembleItem prefix keyReader m = mapMaybe conv . filter chk $ M.toList m
    where
      chk (k, _) = T.isPrefixOf prefix k
      conv (k, v) = (,) <$> parse k <*> fromValue v
      parse k = keyReader $ T.drop (len+1) k
      len = T.length prefix



-------------------------------------------------------------------------------
collect :: (Ord k, F.Foldable t)
        => (a -> k)
        -- ^ key in collected map
        -> (a -> v)
        -- ^ value in collected map
        -> (v -> v -> v)
        -- ^ collapse function for value
        -> t a
        -- ^ something I can traverse, e.g. [a]
        -> M.Map k v
collect k v f as = F.foldr step M.empty as
    where
      step a r = M.insertWith' f (k a) (v a) r



combine :: Lens' s a  -> (a -> a -> a) -> s -> s -> s -> s
combine l f a b c = c & l .~ f (a ^. l) (b ^. l)


-------------------------------------------------------------------------------
-- | Serialize based round-trip from ByteString.
pSerialize :: SL.Serialize a => Prism' B.ByteString a
pSerialize = prism' SL.encode (hush . SL.decode)



-------------------------------------------------------------------------------
makeLenses ''DynSc
makeLenses ''DynSer
makeLenses ''DynHash
makeLenses ''DynUuid
-------------------------------------------------------------------------------


