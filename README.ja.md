<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# Databricksコネクタ

Databricks用のネイティブIrodoriテーブルコネクタ拡張。

このクレートは、コネクタのメタデータ、ネイティブABIエクスポート、およびIrodori拡張マーケットプレイスで使用されるドライバ実装をパッケージ化しています。

## コネクタ

- 拡張ID: `irodori.databricks`
- エンジンID: `databricks`
- ワイヤープロトコル: `jdbc`
- デフォルトポート: `443`
- ネイティブABI: `irodori.connector.native.v1`
- ドライバリンク済み: `yes`
- マーケットプレイスの公開範囲: `public`
- パッケージバージョン: `0.1.3`

このパッケージはコネクタのメタデータとネイティブドライバを直接使用し、デスクトップアダプタのスナップショットは必要ありません。

コネクタのメタデータは`connector.config.json`と`irodori.extension.json`に格納されています。
Rustクレートは`src/lib.rs`からネイティブABIをエクスポートし、`irodori-connector-abi`を共有JSON/バッファヘルパーとして使用し、コネクタの動作は`src/driver.rs`に保持しています。

## 接続メタデータ

- エンドポイントモード: `hostPort`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLSサポート: `yes`
- TLS必須（デフォルト）: `yes`
- カスタムドライバオプション: `yes`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `host` | ホスト | `string` | yes |
| `port` | ポート | `number` | no |
| `httpPath` | HTTPパス | `string` | yes |
| `database` | スキーマ | `string` | no |
| `catalog` | カタログ | `string` | no |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な資格情報フィールドをレンダリングできます。必要に応じて、`options`を通じてドライバ固有またはプロバイダー固有の値を渡すことも可能です。

| 認証方法 | ラベル | 種類 | シークレットの用途 |
| --- | --- | --- | --- |
| `none` | 認証なし | `none` | なし |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `databricksPersonalAccessToken` | Databricks個人アクセストークン | `token` | `token` |
| `databricksOAuthToken` | Databricks OAuthトークン | `oauth2` | `token` |
| `databricksOAuthU2M` | Databricks OAuthユーザー対マシン | `browserSso` | `password` |
| `databricksOAuthM2M` | Databricks OAuthマシン対マシン | `oauth2` | `token` |
| `databricksAzureManagedIdentity` | Databricks Azure管理ID | `managedIdentity` | なし |
| `customDriverOptions` | カスタムドライバオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ネイティブABI呼び出し

| メソッド | 応答 |
| --- | --- |
| `health` | コネクタのヘルス状態、エンジンID、ABIバージョン、ドライバの状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の`irodori.extension.json`を返します。 |
| `config` | 生の`connector.config.json`を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行またはJSON結果を返します。 |
| `metadata` | スキーマ、テーブル、カラム、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは`../target`を共有しており、依存関係は兄弟リポジトリ間で一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージはプラットフォーム固有のネイティブアーティファクトを`dist/native`に配置します。

## ライセンス

0BSD。ほぼすべての目的でこのプロジェクトを使用、コピー、修正、配布できます。