module Database.HDBC.ClickHouse.Data.Reader (readValue) where

import Data.Bits
import Data.IP
import Data.List (intersperse, unfoldr)
import Data.Word
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Protocol
import Network.Socket (Socket)
import Numeric (showHex)

import qualified Database.HDBC.ClickHouse.Codec.Decoder as D

readValue :: Socket -> Column -> Config -> Int -> IO [SqlValue]
readValue sock (StringColumn _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readString sock
readValue sock (Int8Column _) _ numRows =
  readEachValue numRows $ fmap (iToSql . fromIntegral) $ D.readInt8 sock
readValue sock (Int16Column _) _ numRows =
  readEachValue numRows $ fmap (iToSql . fromIntegral) $ D.readInt16 sock
readValue sock (Int32Column _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readInt32 sock
readValue sock (Int64Column _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readInt64 sock
readValue sock (UInt8Column _) _ numRows =
  readEachValue numRows $ fmap (toSql . toWord32) $ D.readWord8 sock
readValue sock (UInt16Column _) _ numRows =
  readEachValue numRows $ fmap (toSql . toWord32) $ D.readWord16 sock
readValue sock (UInt32Column _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readWord32 sock
readValue sock (UInt64Column _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readWord64 sock
readValue sock (Float32Column _) _ numRows =
  readEachValue numRows $ fmap (toSql . toDouble) $ D.readFloat32 sock
readValue sock (Float64Column _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readFloat64 sock
readValue sock (DateColumn _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readDate sock -- TODO: timezone
readValue sock (DateTimeColumn _) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readDateTime sock -- TODO: timezone
readValue sock (FixedStringColumn _ size) _ numRows =
  readEachValue numRows $ fmap toSql $ D.readFixedString sock size
readValue sock (UUIDColumn _) _ numRows =
  readEachValue numRows read
    where
      read = do
        bytes <- mapM (\_ -> fmap fromIntegral $ D.readWord8 sock) [1..16]
        let uuids = foldl toUUID ("", "", "", "", "") $ zip bytes [0..]
        return $ toSql $ concat $ intersperse "-" $ tapleTolist uuids
      toUUID (uuid1, uuid2, uuid3, uuid4, uuid5) (byte, index)
        | index >= 4  && index <= 7  = (toHex byte uuid1, uuid2, uuid3, uuid4, uuid5)
        | index >= 2  && index <= 3  = (uuid1, toHex byte uuid2, uuid3, uuid4, uuid5)
        | index >= 0  && index <= 1  = (uuid1, uuid2, toHex byte uuid3, uuid4, uuid5)
        | index >= 14 && index <= 15 = (uuid1, uuid2, uuid3, toHex byte uuid4, uuid5)
        | index >= 8  && index <= 13 = (uuid1, uuid2, uuid3, uuid4, toHex byte uuid5)
      toHex b s
        | b <= 15   = '0' : (showHex b s)
        | otherwise = showHex b s
      tapleTolist (uuid1, uuid2, uuid3, uuid4, uuid5) = [uuid1, uuid2, uuid3, uuid4, uuid5]
readValue sock (IPv4Column _) _ numRows =
  readEachValue numRows read
    where
      read = do
        bytes <- mapM (\_ -> fmap fromIntegral $ D.readWord8 sock) [1..4]
        let ip = toIPv4 $ reverse bytes
        return $ toSql $ show ip
readValue sock (IPv6Column _) _ numRows =
  readEachValue numRows read
    where
      read = do
        bytes <- mapM (\_ -> fmap fromIntegral $ D.readWord8 sock) [1..16]
        let ip = toIPv6 $ unfoldr f bytes
        return $ toSql $ show ip
      f (x:y:zs) = Just ((x `shiftL` 8) .|. y, zs)
      f _        = Nothing
readValue sock (ArrayColumn _ itemType) config numRows = do
  offsetSum <- mapM (\_ -> fmap fromIntegral $ D.readWord64 sock) [1..numRows]
  let offsets = zipWith (-) offsetSum $ (0:offsetSum)
  fmap (map $ joinSqlValues config) $ mapM (\offset -> readValue sock itemType config offset) offsets
readValue sock (NullableColumn _ itemType) config numRows = do
  isNulls <- mapM (\_ -> D.readBool sock) [1..numRows]
  values <- readValue sock itemType config numRows
  return $ map f $ zip values isNulls
    where
      f (_, True) = SqlNull
      f (v, _   ) = v

readEachValue numRows f =
  mapM (\_ -> f) [1..numRows]

toWord32 :: Integral a => a -> Word32
toWord32 = fromIntegral

toDouble :: Float -> Double
toDouble = realToFrac
