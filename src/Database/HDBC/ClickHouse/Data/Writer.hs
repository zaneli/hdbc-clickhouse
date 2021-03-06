module Database.HDBC.ClickHouse.Data.Writer (encodeValue) where

import Data.Char (toUpper)
import Data.Either
import Data.IP (fromIPv4, fromIPv6b)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column
import Database.HDBC.ClickHouse.Protocol (splitSqlValue, Config)
import Text.Printf

import qualified Codec.Binary.UTF8.String as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Codec.Encoder as E

encodeValue :: Column -> SqlValue -> Config -> Either String B.ByteString
encodeValue (StringColumn _) (SqlString v) _ =
  Right $ E.encodeString v

encodeValue column@(Int8Column _) value@(SqlInt32 v) _ =
  checkRange column value v B.singleton (-128) 127
encodeValue column@(Int8Column _) value@(SqlInt64 v) _ =
  checkRange column value v B.singleton (-128) 127
encodeValue column@(Int8Column _) value@(SqlInteger v) _ =
  checkRange column value v B.singleton (-128) 127

encodeValue column@(Int16Column _) value@(SqlInt32 v) _ =
  checkRange column value v E.encodeInt16 (-32768) 32767
encodeValue column@(Int16Column _) value@(SqlInt64 v) _ =
  checkRange column value v E.encodeInt16 (-32768) 32767
encodeValue column@(Int16Column _) value@(SqlInteger v) _ =
  checkRange column value v E.encodeInt16 (-32768) 32767

encodeValue (Int32Column _) (SqlInt32 v) _ =
  Right $ E.encodeInt32 v
encodeValue column@(Int32Column _) value@(SqlInt64 v) _ =
  checkRange column value v E.encodeInt32 (-2147483648) 2147483647
encodeValue column@(Int32Column _) value@(SqlInteger v) _ =
  checkRange column value v E.encodeInt32 (-2147483648) 2147483647

encodeValue (Int64Column _) (SqlInt64 v) _ =
  Right $ E.encodeInt64 v
encodeValue column@(Int64Column _) value@(SqlInteger v) _ =
  checkRange column value v E.encodeInt64 (-9223372036854775808) 9223372036854775807

encodeValue column@(UInt8Column _) value@(SqlWord32 v) _ =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlWord64 v) _ =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlInt32 v) _ =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlInt64 v) _ =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlInteger v) _ =
  checkRange column value v B.singleton 0 255

encodeValue column@(UInt16Column _) value@(SqlWord32 v) _ =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlWord64 v) _ =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlInt32 v) _ =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlInt64 v) _ =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlInteger v) _ =
  checkRange column value v E.encodeWord16 0 65535

encodeValue (UInt32Column _) (SqlWord32 v) _ =
  Right $ E.encodeWord32 v
encodeValue column@(UInt32Column _) value@(SqlWord64 v) _ =
  checkRange column value v E.encodeWord32 0 4294967295
encodeValue column@(UInt32Column _) value@(SqlInt32 v) _ =
  checkRange column value v E.encodeWord32 0 2147483647 -- Literal 4294967295 is out of the GHC.Int.Int32 range -2147483648..2147483647
encodeValue column@(UInt32Column _) value@(SqlInt64 v) _ =
  checkRange column value v E.encodeWord32 0 4294967295
encodeValue column@(UInt32Column _) value@(SqlInteger v) _ =
  checkRange column value v E.encodeWord32 0 4294967295

encodeValue (UInt64Column _) (SqlWord64 v) _ =
  Right $ E.encodeWord64 v
encodeValue column@(UInt64Column _) value@(SqlInt64 v) _ =
  checkRange column value v E.encodeWord64 0 9223372036854775807 -- Literal 18446744073709551615 is out of the GHC.Int.Int64 range -9223372036854775808..9223372036854775807
encodeValue column@(UInt64Column _) value@(SqlInteger v) _ =
  checkRange column value v E.encodeWord64 0 18446744073709551615

encodeValue column@(Float32Column _) value@(SqlDouble v) _ =
  let f = (realToFrac v)::Float in
  if (isNaN f || isInfinite f)
    then Left $ printf "unsupported value: column=%s, value=%s" (show column) (show value)
    else Right $ E.encodeFloat f
encodeValue (Float64Column _) (SqlDouble v) _ =
  Right $ E.encodeDouble v

encodeValue (DateColumn _) (SqlUTCTime v) _ =
  Right $ E.encodeInt16 $ fromIntegral $ truncate $ (\n -> n / 24 / 3600) $ toRational $ utcTimeToPOSIXSeconds v
encodeValue (DateTimeColumn _) (SqlUTCTime v) _ =
  Right $ E.encodeInt32 $ fromIntegral $ truncate $ toRational $ utcTimeToPOSIXSeconds v

encodeValue column@(UUIDColumn _) value@(SqlString v) _ = do
  uuid <- maybe (Left $ printf "unsupported value: column=%s, value=%s" (show column) (show value)) Right $ toUUID v
  return $ B.concat $ reverse $ map (\i -> toHexBytes (uuid !! i) []) [3, 4, 0, 1, 2]

encodeValue (IPv4Column _) (SqlString v) _ =
  Right $ B.concat $ map (B.singleton . fromIntegral) $ reverse $ fromIPv4 $ read v
encodeValue (IPv6Column _) (SqlString v) _ =
  Right $ B.concat $ map (B.singleton . fromIntegral) $ fromIPv6b $ read v

encodeValue (FixedStringColumn _ size) (SqlString v) _ =
  Right $ B8.pack $ C.encodeString $ (take size v) ++ (replicate (size - (length v)) '\0')

encodeValue (ArrayColumn _ item) value config = do
  values <- splitSqlValue config value
  (encodedValues, lowerDepthSizes) <- encodeValues item values
  let encodedSizes = map (E.encodeWord64 . fromIntegral) $ ((length values) : lowerDepthSizes)
  return $ (B.concat encodedSizes) `B.append` (B.concat encodedValues)
    where
      encodeValues :: Column -> [SqlValue] -> Either String ([B.ByteString], [Int])
      encodeValues (ArrayColumn _ item) values = do
        lowerDepthValues <-  mapM (splitSqlValue config) values
        let currentDepthSizes = tail $ scanl (+) 0 $ map length lowerDepthValues
        encodedValuesWithSizes <- mapM (encodeValues item) lowerDepthValues
        let encodedValues = concat $ map (\(v, _) -> v) encodedValuesWithSizes
        let lowerDepthSizes = concat $ map (\(_, s) -> s) encodedValuesWithSizes
        return (encodedValues, currentDepthSizes ++ lowerDepthSizes)
      encodeValues item values = do
        encodeds <- mapM (\v -> encodeValue item v config) values
        return (encodeds, [])

encodeValue (NullableColumn _ item) SqlNull config =
  fmap (\v -> B.singleton 1 `B.append` v) (encodeValue item (nullValue item) config)
encodeValue (NullableColumn _ item) value config =
  fmap (\v -> B.singleton 0 `B.append` v) (encodeValue item value config)

encodeValue _ (SqlByteString v) _ =
  Right v

encodeValue column value _ =
  Left $ printf "unsupported type: column=%s, value=%s" (show column) (show value)

checkRange :: (Integral a, Integral b) => Column -> SqlValue -> a -> (b -> B.ByteString) -> a -> a -> Either String B.ByteString
checkRange column value n f min max =
  if (n >= min && n <= max)
    then Right $ f $ fromIntegral n
    else Left $ printf "unsupported value: column=%s, value=%s" (show column) (show value)

toUUID :: String -> Maybe [String]
toUUID v =
  if (length uuid /= 5 ||
      length (uuid !! 0) /= 8 ||
      length (uuid !! 1) /= 4 ||
      length (uuid !! 2) /= 4 ||
      length (uuid !! 3) /= 4 ||
      length (uuid !! 4) /= 12)
    then Nothing
    else Just uuid
    where
      uuid = foldr f [] v
      f '-' xs   = [] : xs
      f c (x:xs) = (c:x):xs
      f c []     = [[c]]

toHexBytes "" xs = B.concat $ map B.singleton xs
toHexBytes s  xs = let [s1, s2] = (take 2 s) in
                   toHexBytes (drop 2 s) ((fromIntegral $ (hexChar $ toUpper s1) * 16 + (hexChar $ toUpper s2)):xs)
  where
    hexChar '0' = 0
    hexChar '1' = 1
    hexChar '2' = 2
    hexChar '3' = 3
    hexChar '4' = 4
    hexChar '5' = 5
    hexChar '6' = 6
    hexChar '7' = 7
    hexChar '8' = 8
    hexChar '9' = 9
    hexChar 'A' = 10
    hexChar 'B' = 11
    hexChar 'C' = 12
    hexChar 'D' = 13
    hexChar 'E' = 14
    hexChar 'F' = 15
