# BGP 数据下载与解包程序

一个 运行于 Action 的 Python 程序，用于从 [RouteViews](http://archive.routeviews.org/) 自动下载最新的 BGP（Border Gateway Protocol）数据文件，并解析生成每个自治系统（AS）的前缀列表。

## ✨ 功能特性

- ✅ **自动获取最新数据**：智能遍历年/月/日目录结构，定位最新可用的 BGP 数据文件
- ✅ **生成 AS 前缀**：提取每个 AS 的前缀列表，并生成所有前缀的汇总文件


### 直接下载预生成的前缀文件
为减轻 RouteViews 服务器的压力，建议优先使用已生成的前缀文件：
- **全部前缀**：[all_as_prefixes.txt](https://github.com/Liyouran-center/routeviews-fetcher/raw/refs/heads/ip-data/all_as_prefixes.txt)

当前的全部前缀为本人常用的需要分流的AS IP

如需新增或修改 AS 的前缀数据，请通过 **Issue** 或 **Pull Request** 提交，审核后将更新至仓库。

## 📊 常见 AS 号示例

| AS号      | 名称                       |
|-----------|----------------------------|
| AS13335   | Cloudflare                 |
| AS15169   | Google                     |
| AS32934   | Facebook                   |
| AS20473   | Vultr                      |
| AS54113   | Fastly CDN                 |
| AS21050   | Fast                       |
| AS16509   | Amazon                     |
| AS2906    | Netflix                    |
| AS8075    | Microsoft                  |
| AS51894   | Mikrotikls SIA             |
| AS328633  | MikroTikSA Networks CC     |

## 📡 BGP 数据源

数据来自 [RouteViews 项目](http://www.routeviews.org/)，该项目在全球多个位置收集 BGP 路由表快照，并提供 MRT 格式的原始数据。本程序从 [RouteViews 归档服务器](http://archive.routeviews.org/) 获取数据，确保数据源的可靠性和稳定性。

## 📄 许可证

本项目采用 [MIT 许可证](LICENSE)。

## 🤝 贡献

欢迎通过 Issue 报告问题或提交改进建议。
如果您希望为新的 AS 添加前缀数据，请优先提交 PR 或 Issue，以便集中维护，避免重复下载对 RouteViews 服务器造成压力。

## 🔗 相关资源

- [RouteViews Project](http://www.routeviews.org/)
- [RouteViews Archive](http://archive.routeviews.org/)
- [BGP 协议 (RFC 4271)](https://tools.ietf.org/html/rfc4271)
- [MRT 格式 (RFC 6396)](https://tools.ietf.org/html/rfc6396)

