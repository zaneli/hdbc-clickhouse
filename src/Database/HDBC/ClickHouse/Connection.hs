module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Database.HDBC
import Database.HDBC.Types
import Database.HDBC.DriverUtils
import Control.Exception
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (sendAll)
import Database.HDBC.ClickHouse.Protocol
import Database.HDBC.ColTypes

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
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
    Impl.prepare = fprepare sock config,
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
  throwIO $ userError "not implemented"

frollback :: IO ()
frollback =
  throwIO $ userError "not implemented"

frun :: Socket -> String -> [SqlValue] -> IO Integer
frun sock sql args =
  throwIO $ userError "not implemented"

frunRaw :: Socket -> ServerInfo -> Config -> String -> IO ()
frunRaw sock serverInfo config sql = do
  Query.send sock sql serverInfo config
  return ()

fprepare :: Socket -> Config -> String -> IO Statement
fprepare sock config sql =
  throwIO $ userError "not implemented"

fgetTables :: Socket -> ServerInfo -> Config -> IO [String]
fgetTables sock serverInfo config = do
  tables <- Query.send sock "show tables" serverInfo config
  return $ concat $ map (\(_, (vs)) -> (map fromSql vs)) tables

fdescribeTable :: Socket -> ServerInfo -> Config -> String -> IO [(String, SqlColDesc)]
fdescribeTable sock serverInfo config sql =
  throwIO $ userError "not implemented"
