# Gridflare.xyz

Gridflare.xyz was a lightning network explorer and channel recommendation engine launched in 2023 and archived in 2026.

## Running locally

### Download the repository

```sh
git clone https://github.com/Gridflare/gridflare.xyz.git
cd gridflare.xyz
```

### Link your credentials

I recommend linking to your credentials with `ln -s`, but copying and pasting into `data/credentials` will work too.

```
mkdir -p data/credentials
cd data/credentials
ln -s ~/.lnd/tls.cert
ln -s ~/.lnd/data/chain/bitcoin/mainnet/readonly.macaroon
ln -s ~/.lnd/data/chain/bitcoin/mainnet/admin.macaroon
```

The readonly macaroon is sufficient to run the website, extra permissions are only required if `randomgossipconnections` is enabled (see below).

### Configure the server

Edit processor/node.conf.

1. Set `server` to your node ip.
2. If you use Neutrino, set `use_include_unannounced_workaround = 1` and `listener_node_pub`.
3. Optional, for better graph knowledge over time, set `randomgossipconnections = 1` and `macpath` to `admin.macaroon`.

### Start the website

```sh
docker compose -f docker-compose-dev.yml up
```

Go to `localhost:7080` in your web browser.

Additional debug logs are located in `processor/processor.log`.

```sh
tail -f processor/processor.log
```

