#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BGP数据下载与解包程序
从RouteViews (http://archive.routeviews.org/) 下载最新的BGP数据文件
支持自动解包（.gz, .bz2 等压缩格式）
"""

import os
import sys
import re
import gzip
import bz2
import shutil
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import html.parser

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LinkExtractor(html.parser.HTMLParser):
    """HTML解析器，用于提取链接"""
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href':
                    self.links.append(value)


class BGPDownloader:
    """BGP文件下载器"""

    # RouteViews数据源配置
    BASE_URL = "http://archive.routeviews.org/route-views2/bgpdata/"
    TIMEOUT = 600  # 请求超时时间（秒）- 设置为10分钟
    MAX_RETRIES = 5  # 最大重试次数

    def __init__(self, output_dir: str = "./bgp_data"):
        """
        初始化下载器

        Args:
            output_dir: 输出目录路径
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"输出目录设置为: {self.output_dir.absolute()}")

    def fetch_page_content(self, url: str) -> str:
        """
        获取网页内容

        Args:
            url: 网页URL

        Returns:
            网页HTML内容

        Raises:
            URLError: 网络错误
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            request = Request(url, headers=headers)
            with urlopen(request, timeout=self.TIMEOUT) as response:
                return response.read().decode('utf-8')
        except HTTPError as e:
            logger.error(f"HTTP错误 {e.code}: {url}")
            raise
        except URLError as e:
            logger.error(f"连接错误: {e.reason}")
            raise

    def extract_links(self, html_content: str) -> list:
        """
        从HTML中提取所有链接

        Args:
            html_content: HTML内容

        Returns:
            链接列表
        """
        parser = LinkExtractor()
        parser.feed(html_content)
        return parser.links

    def get_latest_bgp_files(self) -> list:
        """
        获取最新的BGP数据文件列表
        按目录结构：YYYY.MM/DD/

        Returns:
            BGP文件信息列表，包含 (年, 月, 日, 文件名, 下载URL)
        """
        try:
            logger.info("正在获取最新BGP文件列表...")
            html_content = self.fetch_page_content(self.BASE_URL)
            links = self.extract_links(html_content)

            # 提取年月目录（格式：YYYY.MM/）
            year_month_pattern = r'(\d{4}\.\d{2})/'
            year_months = {m.group(1) for m in re.finditer(year_month_pattern, ' '.join(links))}
            year_months = sorted(year_months, reverse=True)

            if not year_months:
                logger.warning("未找到YYYY.MM目录")
                return []

            logger.info(f"找到 {len(year_months)} 个月份，尝试获取RIBS数据...")

            # 尝试找到有RIBS数据的最新月份
            bgp_files = []
            for ym in year_months[:12]:  # 尝试最新的12个月
                logger.info(f"尝试年月: {ym}")

                # 尝试 RIBS 目录
                ribs_url = f"{self.BASE_URL}{ym}/RIBS/"
                try:
                    html_content = self.fetch_page_content(ribs_url)
                    links = self.extract_links(html_content)

                    # 过滤BGP数据文件
                    for link in links:
                        # 匹配BGP文件名: rib.YYYYMMDD.HHMM.bz2 或 rib.YYYYMMDD.HHMM.gz
                        if re.search(r'rib\.\d{8}\.\d{4}\.(bz2|gz)$', link):
                            year, month = ym.split('.')
                            file_info = {
                                'year': year,
                                'month': month,
                                'filename': link.strip('/'),
                                'url': f"{ribs_url}{link.strip('/')}"
                            }
                            bgp_files.append(file_info)

                    # 如果找到文件就停止搜索
                    if bgp_files:
                        bgp_files.sort(key=lambda x: x['filename'], reverse=True)
                        logger.info(f"✓ 在 {ym}/RIBS/ 找到 {len(bgp_files)} 个BGP文件")
                        logger.debug(f"  最新的: {bgp_files[0]['filename']}")
                        break
                except Exception as e:
                    logger.debug(f"  {ym}/RIBS/ 获取失败: {e}")
                    continue

            if not bgp_files:
                logger.warning(f"无法在最新12个月的RIBS目录中找到BGP文件，尝试UPDATES...")
                # 尝试 UPDATES 目录
                for ym in year_months[:3]:
                    updates_url = f"{self.BASE_URL}{ym}/UPDATES/"
                    try:
                        html_content = self.fetch_page_content(updates_url)
                        links = self.extract_links(html_content)

                        for link in links:
                            if re.search(r'updates\.\d{8}\.\d{4}\.(bz2|gz)$', link):
                                year, month = ym.split('.')
                                file_info = {
                                    'year': year,
                                    'month': month,
                                    'filename': link.strip('/'),
                                    'url': f"{updates_url}{link.strip('/')}"
                                }
                                bgp_files.append(file_info)

                        if bgp_files:
                            bgp_files.sort(key=lambda x: x['filename'], reverse=True)
                            logger.info(f"✓ 在 {ym}/UPDATES/ 找到 {len(bgp_files)} 个文件")
                            break
                    except Exception as e:
                        logger.debug(f"  {ym}/UPDATES/ 获取失败: {e}")
                        continue

            return bgp_files

        except Exception as e:
            logger.error(f"获取BGP文件列表失败: {e}")
            return []

    def download_file(self, url: str, filename: str) -> bool:
        """
        下载单个文件（带重试机制）

        Args:
            url: 文件URL
            filename: 本地保存的文件名

        Returns:
            是否下载成功
        """
        filepath = self.output_dir / filename

        for attempt in range(self.MAX_RETRIES):
            try:
                if attempt > 0:
                    logger.info(f"重试下载 ({attempt}/{self.MAX_RETRIES}): {filename}")
                else:
                    logger.info(f"开始下载: {filename}")

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                request = Request(url, headers=headers)

                with urlopen(request, timeout=self.TIMEOUT) as response:
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    chunk_size = 1024 * 256  # 256KB chunks
                    last_logged = 0

                    with open(filepath, 'wb') as f:
                        while True:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)

                            # 每增加10MB时打印一次进度
                            if total_size and downloaded - last_logged > 10 * 1024 * 1024:
                                percent = (downloaded / total_size) * 100
                                logger.info(f"  进度: {downloaded / 1024 / 1024:.1f}MB / {total_size / 1024 / 1024:.1f}MB ({percent:.1f}%)")
                                last_logged = downloaded

                # 验证下载大小
                actual_size = filepath.stat().st_size
                if total_size > 0:
                    completeness = (actual_size / total_size) * 100
                    if completeness < 95:  # 如果小于95%则认为不完整
                        logger.warning(f"下载不完整: {completeness:.1f}% ({actual_size}/{total_size})")
                        if attempt < self.MAX_RETRIES - 1:
                            filepath.unlink()  # 删除不完整文件，准备重试
                            continue
                        else:
                            logger.error(f"多次重试后仍无法完整下载文件")
                            return False

                logger.info(f"文件下载成功: {filepath} ({actual_size / 1024 / 1024:.1f}MB)")
                return True

            except Exception as e:
                logger.warning(f"下载尝试 {attempt + 1} 失败: {e}")
                if filepath.exists():
                    try:
                        filepath.unlink()
                    except:
                        pass

                if attempt < self.MAX_RETRIES - 1:
                    continue
                else:
                    logger.error(f"下载失败: {e}")
                    return False

        return False

    def extract_file(self, filepath: Path) -> Optional[Path]:
        """
        解包文件

        Args:
            filepath: 文件路径

        Returns:
            解包后的文件路径，如果失败则返回None
        """
        try:
            if str(filepath).endswith('.gz'):
                output_path = filepath.parent / filepath.stem
                logger.info(f"正在解包 .gz 文件: {filepath.name}")

                with gzip.open(filepath, 'rb') as f_in:
                    with open(output_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                logger.info(f"解包成功: {output_path}")
                return output_path

            elif str(filepath).endswith('.bz2'):
                output_path = filepath.parent / filepath.stem
                logger.info(f"正在解包 .bz2 文件: {filepath.name}")

                with bz2.open(filepath, 'rb') as f_in:
                    with open(output_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                logger.info(f"解包成功: {output_path}")
                return output_path

            else:
                logger.info(f"文件不需要解包: {filepath.name}")
                return filepath

        except Exception as e:
            logger.error(f"解包失败: {e}")
            return None

    def parse_mrt_file(self, filepath: Path, output_format: str = 'txt') -> Optional[Path]:
        """
        使用mrt2bgpdump.py解析MRT文件

        Args:
            filepath: MRT文件路径
            output_format: 输出格式 ('txt', 'json' 等)

        Returns:
            解析后的输出文件路径，如果失败则返回None
        """
        try:
            if not filepath.exists():
                logger.error(f"文件不存在: {filepath}")
                return None

            logger.info(f"正在解析MRT文件: {filepath.name}")

            # 生成输出文件名
            output_path = filepath.with_suffix(f'.{output_format}')

            # 检查mrt2bgpdump.py是否存在
            mrt_script = Path(__file__).parent / 'mrt2bgpdump.py'

            if not mrt_script.exists():
                logger.warning(f"mrt2bgpdump.py不存在，跳过MRT解析: {mrt_script}")
                return None

            try:
                # 调用mrt2bgpdump.py
                cmd = [sys.executable, str(mrt_script), str(filepath), '-O', str(output_path)]
                logger.info(f"执行命令: {' '.join(cmd)}")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=1200,  # 10分钟超时
                    text=True
                )

                if result.returncode != 0:
                    logger.warning(f"MRT解析失败 (代码: {result.returncode})")
                    if result.stderr:
                        logger.warning(f"错误信息: {result.stderr[:500]}")
                    return None

                if output_path.exists() and output_path.stat().st_size > 0:
                    size_mb = output_path.stat().st_size / 1024 / 1024
                    logger.info(f"✓ MRT解析成功: {output_path.name} ({size_mb:.1f}MB)")
                    return output_path
                else:
                    logger.warning(f"输出文件为空或不存在: {output_path}")
                    return None

            except subprocess.TimeoutExpired:
                logger.error("MRT解析超时（20分钟）")
                return None
            except FileNotFoundError:
                logger.error("无法找到Python解释器或脚本")
                return None

        except Exception as e:
            logger.error(f"MRT解析异常: {e}")
            return None

    def download_and_extract(self, file_info: dict, keep_compressed: bool = False) -> Optional[Path]:
        """
        下载并解包BGP文件

        Args:
            file_info: 文件信息字典，包含 url 和 filename
            keep_compressed: 是否保留压缩文件

        Returns:
            最终文件路径，如果失败则返回None
        """
        filename = file_info['filename']
        url = file_info['url']

        # 下载文件
        if not self.download_file(url, filename):
            return None

        filepath = self.output_dir / filename

        # 解包文件
        extracted_path = self.extract_file(filepath)

        # 可选：删除压缩文件
        if extracted_path and not keep_compressed and filepath != extracted_path:
            try:
                filepath.unlink()
                logger.info(f"已删除压缩文件: {filepath.name}")
            except Exception as e:
                logger.warning(f"删除压缩文件失败: {e}")

        return extracted_path

    def extract_and_parse(self, compressed_file: str, keep_compressed: bool = False, parse_mrt: bool = True) -> Optional[Path]:
        """
        仅解包和解析已有的压缩文件（跳过下载）

        Args:
            compressed_file: 压缩文件路径或文件名
            keep_compressed: 是否保留压缩文件
            parse_mrt: 是否使用mrt2bgpdump解析MRT文件

        Returns:
            最终文件的路径，如果失败则返回None
        """
        filepath = Path(compressed_file)

        # 如果只提供了文件名，在输出目录中查找
        if not filepath.is_absolute() and not filepath.parent.exists():
            filepath = self.output_dir / compressed_file

        if not filepath.exists():
            logger.error(f"文件不存在: {compressed_file}")
            return None

        logger.info("=" * 50)
        logger.info("仅解包和解析模式")
        logger.info("=" * 50)
        logger.info(f"处理文件: {filepath.name}")

        # 解包文件
        extracted_path = self.extract_file(filepath)

        if not extracted_path:
            logger.error("✗ 解包失败")
            return None

        # 可选：删除压缩文件
        if not keep_compressed and filepath != extracted_path:
            try:
                filepath.unlink()
                logger.info(f"已删除压缩文件: {filepath.name}")
            except Exception as e:
                logger.warning(f"删除压缩文件失败: {e}")

        # 如果启用MRT解析，则调用mrt2bgpdump.py
        if parse_mrt:
            logger.info("\n" + "=" * 50)
            logger.info("开始解析MRT文件...")
            logger.info("=" * 50)
            parsed_path = self.parse_mrt_file(extracted_path, output_format='txt')
            if parsed_path:
                logger.info(f"✓ MRT文件已解析: {parsed_path.absolute()}")

        logger.info("=" * 50)
        logger.info(f"✓ 操作完成！文件位置: {extracted_path.absolute()}")
        logger.info("=" * 50)

        return extracted_path

    def run(self, keep_compressed: bool = False, parse_mrt: bool = True, skip_download: bool = False) -> Optional[Path]:
        """
        主流程：获取最新BGP文件并下载解包

        Args:
            keep_compressed: 是否保留压缩文件
            parse_mrt: 是否使用mrt2bgpdump解析MRT文件
            skip_download: 是否跳过下载，仅使用本地已有的文件

        Returns:
            最终解包文件的路径
        """
        logger.info("=" * 50)
        logger.info("BGP数据下载程序启动")
        logger.info("=" * 50)

        if skip_download:
            logger.warning("警告: 跳过下载，将查找本地文件")
            logger.info("=" * 50)

            # 第1步：查找压缩文件
            logger.info("第1步: 查找本地压缩文件...")
            compressed_files = list(self.output_dir.glob('*.bz2')) + list(self.output_dir.glob('*.gz'))

            if compressed_files:
                # 使用最新修改时间的压缩文件
                latest_file = max(compressed_files, key=lambda p: p.stat().st_mtime)
                logger.info(f"✓ 找到压缩文件: {latest_file.name} ({latest_file.stat().st_size / 1024 / 1024:.1f}MB)")

                result_path = self.extract_and_parse(str(latest_file), keep_compressed, parse_mrt)
                return result_path

            logger.info("  未找到压缩文件")

            # 第2步：查找已解压的MRT文件
            logger.info("第2步: 查找已解压的MRT文件...")
            # 寻找常见的MRT文件模式 (rib.*.0000, bview.*.0000 等)
            extracted_files = []
            for pattern in ['rib.????????.*', 'bview.????????.*']:
                extracted_files.extend(self.output_dir.glob(pattern))

            # 排除.bz2和.gz文件
            extracted_files = [f for f in extracted_files if f.suffix not in ['.bz2', '.gz', '.txt']]

            if extracted_files:
                # 使用最新修改时间的文件
                latest_file = max(extracted_files, key=lambda p: p.stat().st_mtime)
                logger.info(f"✓ 找到MRT文件: {latest_file.name} ({latest_file.stat().st_size / 1024 / 1024:.1f}MB)")

                # 直接进行MRT解析，不需要解包
                logger.info("=" * 50)
                logger.info("跳过解包（已是解压文件），直接进行MRT解析")
                logger.info("=" * 50)

                if parse_mrt:
                    parsed_path = self.parse_mrt_file(latest_file, output_format='txt')
                    if parsed_path:
                        logger.info(f"✓ MRT文件已解析: {parsed_path.absolute()}")

                logger.info("=" * 50)
                logger.info(f"✓ 操作完成！文件位置: {latest_file.absolute()}")
                logger.info("=" * 50)

                return latest_file

            logger.info("  未找到MRT文件")
            logger.error("✗ 未找到本地压缩文件或MRT文件，请检查输出目录 ({})".format(self.output_dir.absolute()))
            return None

        # 获取最新BGP文件列表
        bgp_files = self.get_latest_bgp_files()

        if not bgp_files:
            logger.error("没有找到可用的BGP文件")
            return None

        # 下载最新的文件
        latest_file = bgp_files[0]
        logger.info(f"准备下载最新文件: {latest_file['filename']}")

        result_path = self.download_and_extract(latest_file, keep_compressed)

        # 如果启用MRT解析，则调用mrt2bgpdump.py
        if result_path and parse_mrt:
            logger.info("\n" + "=" * 50)
            logger.info("开始解析MRT文件...")
            logger.info("=" * 50)
            parsed_path = self.parse_mrt_file(result_path, output_format='txt')
            if parsed_path:
                logger.info(f"✓ MRT文件已解析: {parsed_path.absolute()}")

        logger.info("=" * 50)
        if result_path:
            logger.info(f"✓ 操作完成！文件位置: {result_path.absolute()}")
        else:
            logger.error("✗ 操作失败")
        logger.info("=" * 50)

        return result_path


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description='从RouteViews下载并解包最新BGP数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
例子:
  python download_bgp.py                      # 完整流程：下载 + 解包 + 解析
  python download_bgp.py --skip-download      # 仅解包本地最新压缩文件
  python download_bgp.py --file rib.xxx.bz2  # 解包指定文件
  python download_bgp.py --skip-mrt           # 下载+解包，不解析MRT
  python download_bgp.py -o ./my_data         # 指定输出目录
  python download_bgp.py -k                   # 保留压缩文件
        '''
    )
    parser.add_argument(
        '-o', '--output',
        default='./bgp_data',
        help='输出目录 (默认: ./bgp_data)'
    )
    parser.add_argument(
        '-k', '--keep',
        action='store_true',
        help='保留压缩文件'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='跳过下载，仅处理本地最新的压缩文件'
    )
    parser.add_argument(
        '--file',
        type=str,
        default=None,
        help='指定要处理的本地压缩文件路径（自动启用跳过下载）'
    )
    parser.add_argument(
        '--skip-mrt',
        action='store_true',
        help='跳过MRT文件解析（mrt2bgpdump）'
    )

    args = parser.parse_args()

    try:
        downloader = BGPDownloader(output_dir=args.output)

        # 如果指定了文件，则直接处理该文件
        if args.file:
            logger.info(f"处理指定文件: {args.file}")
            result = downloader.extract_and_parse(
                args.file,
                keep_compressed=args.keep,
                parse_mrt=not args.skip_mrt
            )
        else:
            # 使用run()方法，支持skip_download
            result = downloader.run(
                keep_compressed=args.keep,
                parse_mrt=not args.skip_mrt,
                skip_download=args.skip_download
            )

        if result:
            sys.exit(0)
        else:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n程序被用户中断")
        sys.exit(130)
    except Exception as e:
        logger.error(f"发生错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
