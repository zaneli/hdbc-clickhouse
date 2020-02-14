{-# LANGUAGE FlexibleContexts #-}
module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Database.HDBC
import Database.HDBC.Types
import Database.HDBC.DriverUtils
import Control.Exception
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (sendAll)

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
import qualified Database.HDBC.ClickHouse.Protocol.Hello as Hello
import qualified Database.HDBC.ClickHouse.Protocol.Ping as Ping

connectClickHouse :: String -> Int -> String -> String -> String -> Bool -> IO Impl.Connection
connectClickHouse host port database username password debug =
  mkConn host port database username password debug

mkConn :: String -> Int -> String -> String -> String -> Bool -> IO Impl.Connection
mkConn host port database username password debug = do
  conn <- fconnect host port database username password debug
  return $ Impl.Connection {
    Impl.disconnect = fclose conn,
    Impl.commit = fcommit,
    Impl.rollback = frollback,
    Impl.run = frun conn,
    Impl.runRaw = frunRaw conn,
    Impl.ping = fping conn
  }

fconnect :: String -> Int -> String -> String -> String -> Bool -> IO Socket
fconnect host port database username password debug = withSocketsDo $ do
  addrInfo <- getAddrInfo Nothing (Just host) (Just $ show port)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)

  Hello.send sock database username password debug

  return sock

fping :: Socket -> IO String
fping sock = withSocketsDo $ do
  Ping.send sock

fclose :: Socket -> IO ()
fclose conn = close conn

-- TODO: not implemented
fcommit :: IO ()
fcommit = return ()

-- TODO: not implemented
frollback :: IO ()
frollback = return ()

-- TODO: not implemented
frun :: Socket -> String -> [SqlValue] -> IO Integer
frun conn sql args = do
  return 1

-- TODO: not implemented
frunRaw :: Socket -> String -> IO ()
frunRaw conn sql = do
  return ()
