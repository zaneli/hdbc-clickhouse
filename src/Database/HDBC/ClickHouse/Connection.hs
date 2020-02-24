module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Control.Exception
import Data.List (find)
import Database.HDBC
import Database.HDBC.Types
import Database.HDBC.DriverUtils
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (sendAll)
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Data.ColDesc
import Database.HDBC.ClickHouse.Data.Creation
import Database.HDBC.ClickHouse.Protocol
import Database.HDBC.ColTypes

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
import qualified Database.HDBC.ClickHouse.Statement as Stmt
import qualified Database.HDBC.ClickHouse.Protocol.Hello as Hello
import qualified Database.HDBC.ClickHouse.Protocol.Ping as Ping
import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

connectClickHouse :: Config -> IO Impl.Connection
connectClickHouse config =
  mkConn config

mkConn :: Config -> IO Impl.Connection
mkConn config = do
  (sock, serverInfo) <- fconnect config
  let clientVer = clientVersion clientInfo
  return $ Impl.Connection {
    Impl.disconnect = fclose sock,
    Impl.commit = fcommit,
    Impl.rollback = frollback,
    Impl.run = frun sock,
    Impl.runRaw = frunRaw sock serverInfo config,
    Impl.prepare = Stmt.fprepare sock serverInfo config,
    Impl.clone = connectClickHouse config,
    Impl.hdbcDriverName = "clickhouse",
    Impl.hdbcClientVer = clientVer,
    Impl.proxiedClientName = "clickhouse",
    Impl.proxiedClientVer = clientVer,
    Impl.dbTransactionSupport = True,
    Impl.dbServerVer = serverVersion serverInfo,
    Impl.getTables = fgetTables sock serverInfo config,
    Impl.describeTable = fdescribeTable sock serverInfo config,
    Impl.ping = fping sock
  }

fconnect :: Config -> IO (Socket, ServerInfo)
fconnect config = withSocketsDo $ do
  addrInfo <- getAddrInfo Nothing (Just $ host config ) (Just $ show $ port config)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)

  serverInfo <- Hello.send sock config

  return (sock, serverInfo)

fping :: Socket -> IO String
fping sock = withSocketsDo $ do
  Ping.send sock

fclose :: Socket -> IO ()
fclose sock = close sock

fcommit :: IO ()
fcommit =
  throwIO $ userError "fcommit not implemented"

frollback :: IO ()
frollback =
  throwIO $ userError "frollback not implemented"

frun :: Socket -> String -> [SqlValue] -> IO Integer
frun sock sql args =
  throwIO $ userError "frun not implemented"

frunRaw :: Socket -> ServerInfo -> Config -> String -> IO ()
frunRaw sock serverInfo config sql = do
  Query.send sock sql serverInfo config
  return ()

fgetTables :: Socket -> ServerInfo -> Config -> IO [String]
fgetTables sock serverInfo config = do
  tables <- Query.send sock "show tables" serverInfo config
  return $ concat $ map (\(_, (vs)) -> (map fromSql vs)) tables

fdescribeTable :: Socket -> ServerInfo -> Config -> String -> IO [(String, SqlColDesc)]
fdescribeTable sock serverInfo config table = do
  desc <- Query.send sock ("desc " ++ table) serverInfo config
  let names = find (\(column, values) -> (columnName column == "name") && (not $ null values)) desc
  let types = find (\(column, values) -> (columnName column == "type") && (not $ null values)) desc
  case (names, types) of
    (Just ns, Just ts) -> return $ map f $ zip (map fromSql (snd ns)) $ snd ts
    _ -> throwIO $ userError "table metadata not found"
  where
    f (name, typ) =
      let column = createColumn name $ fromSql typ
          desc   = getSqlColDesc column
      in (name, desc)
