#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BGP AS前缀提取工具
从mrt2bgpdump生成的BGP数据文件中提取指定AS的所有前缀
并将其写入以AS号命名的文件中
"""

import re
import sys
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional
from collections import defaultdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ASPrefixExtractor:
    """BGP AS前缀提取器"""

    def __init__(self, input_file: str, output_dir: str = "./as_prefixes"):
        """
        初始化提取器

        Args:
            input_file: 输入的BGP数据文件（mrt2bgpdump格式）
            output_dir: 输出目录
        """
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # AS前缀映射: AS号 -> 前缀集合
        self.as_prefixes: Dict[int, Set[str]] = defaultdict(set)

        # 统计信息
        self.total_lines = 0
        self.parsed_lines = 0
        self.as_count = 0
        self.prefix_count = 0

        logger.info(f"输入文件: {self.input_file.absolute()}")
        logger.info(f"输出目录: {self.output_dir.absolute()}")

    def parse_bgp_line(self, line: str) -> Optional[tuple]:
        """
        解析BGP行数据

        bgpdump格式示例:
        TIME|TYPE|SUBTYPE|SRC_AS|SRC_IP|PREFIX|AS_PATH|ORIGIN|NEXT_HOP|LOCAL_PREF|MED|ATOMIC_AGG|AGGREGATOR

        或者mrt2bgpdump的简化格式:
        [RIB] TIME | Prefix=x.x.x.x/xx | PeerIdx=x | PathLen=x

        Args:
            line: BGP数据行

        Returns:
            (前缀, AS路径) 元组，如果解析失败返回None
        """
        line = line.strip()
        if not line or line.startswith('#'):
            return None

        try:
            # 尝试解析mrt2bgpdump简化格式
            if '|' in line and 'Prefix=' in line:
                # 格式: [RIB] TIME | Prefix=x.x.x.x/xx | ...
                match = re.search(r'Prefix=([0-9a-f:.]+/\d+)', line)
                if match:
                    prefix = match.group(1)
                    # 如果有AS路径信息，提取最后一个AS（源AS）
                    as_match = re.search(r'ASPath=(\d+(?:\s+\d+)*)', line)
                    if as_match:
                        as_path = as_match.group(1).split()
                        return (prefix, as_path)
                    return (prefix, [])

            # 尝试解析标准bgpdump格式
            elif '|' in line:
                parts = line.split('|')
                if len(parts) >= 7:
                    prefix = parts[5].strip()
                    as_path_str = parts[6].strip()

                    # 验证前缀格式
                    if re.match(r'^[0-9a-f:.]+/\d+$', prefix):
                        # 解析AS路径
                        as_path = as_path_str.split() if as_path_str else []
                        return (prefix, as_path)

            # 尝试解析其他格式（如bgpdump的详细输出）
            else:
                # 匹配 "PREFIX AS_PATH" 格式
                match = re.match(r'^([0-9a-f:.]+/\d+)\s+(\d+(?:\s+\d+)*)?', line)
                if match:
                    prefix = match.group(1)
                    as_path_str = match.group(2) or ''
                    as_path = as_path_str.split() if as_path_str else []
                    return (prefix, as_path)

        except Exception as e:
            logger.debug(f"解析行失败: {line[:80]}... ({e})")

        return None

    def get_as_from_path(self, as_path: List[str], position: str = 'last') -> Optional[int]:
        """
        从AS路径中提取AS号

        Args:
            as_path: AS路径列表
            position: 'first'(首个AS) 或 'last'(最后一个AS，通常是源AS)

        Returns:
            AS号，如果无法提取返回None
        """
        if not as_path:
            return None

        try:
            if position == 'first':
                as_num = int(as_path[0])
            elif position == 'last':
                as_num = int(as_path[-1])
            else:
                return None

            return as_num if as_num > 0 else None
        except (ValueError, IndexError):
            return None

    def extract(self, as_numbers: Optional[List[int]] = None,
                as_position: str = 'last',
                ipv4_only: bool = True) -> Dict[int, int]:
        """
        从输入文件提取数据

        Args:
            as_numbers: 要提取的AS号列表，None表示提取所有AS
            as_position: 'first' 或 'last'（从AS路径中提取哪个AS）
            ipv4_only: 仅提取IPv4前缀

        Returns:
            统计信息字典: {AS号 -> 前缀数量}
        """
        if not self.input_file.exists():
            logger.error(f"文件不存在: {self.input_file}")
            return {}

        logger.info(f"开始处理文件（提取{'所有AS' if not as_numbers else f'{len(as_numbers)}个AS的'}前缀）...")

        try:
            with open(self.input_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    self.total_lines += 1

                    # 显示进度
                    if self.total_lines % 1000000 == 0:
                        logger.info(f"已处理 {self.total_lines} 行...")

                    # 解析行
                    parsed = self.parse_bgp_line(line)
                    if not parsed:
                        continue

                    prefix, as_path = parsed
                    self.parsed_lines += 1

                    # 过滤IPv4
                    if ipv4_only and ':' in prefix:
                        continue

                    # 提取AS号
                    as_num = self.get_as_from_path(as_path, as_position)
                    if as_num is None:
                        continue

                    # 如果指定了AS列表，则只收集这些AS
                    if as_numbers and as_num not in as_numbers:
                        continue

                    # 添加前缀
                    self.as_prefixes[as_num].add(prefix)
                    self.prefix_count += 1

            self.as_count = len(self.as_prefixes)

            logger.info(f"✓ 文件处理完成")
            logger.info(f"  总行数: {self.total_lines}")
            logger.info(f"  已解析行: {self.parsed_lines}")
            logger.info(f"  提取AS数: {self.as_count}")
            logger.info(f"  总前缀数: {self.prefix_count}")

            return {as_num: len(prefixes) for as_num, prefixes in self.as_prefixes.items()}

        except Exception as e:
            logger.error(f"处理文件失败: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def save_results(self, compress: bool = False) -> Dict[int, Path]:
        """
        将提取的前缀保存到文件

        Args:
            compress: 是否压缩IP前缀（使用集合表示法）

        Returns:
            保存文件的映射: {AS号 -> 文件路径}
        """
        logger.info("\n开始保存结果...")
        logger.info("=" * 50)

        saved_files = {}
        total_size = 0

        # 按AS号排序
        for as_num in sorted(self.as_prefixes.keys()):
            prefixes = sorted(self.as_prefixes[as_num])

            # 生成输出文件名
            output_file = self.output_dir / f"AS{as_num}_prefixes.txt"

            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    # # 写入头信息
                    # f.write(f"# AS: {as_num}\n")
                    # f.write(f"# 前缀总数: {len(prefixes)}\n")
                    # f.write(f"# 生成时间: {Path(self.input_file).stat().st_mtime}\n")
                    # f.write("#\n")
                    # f.write("# 前缀列表:\n")
                    # f.write("#\n")

                    # 写入前缀
                    for prefix in prefixes:
                        f.write(f"{prefix}\n")

                file_size = output_file.stat().st_size
                total_size += file_size
                saved_files[as_num] = output_file

                logger.info(f"✓ AS{as_num}: {len(prefixes)} 前缀 ({file_size / 1024:.1f}KB) -> {output_file.name}")

            except Exception as e:
                logger.error(f"✗ AS{as_num}: 保存失败 - {e}")

        logger.info("=" * 50)
        logger.info(f"✓ 保存完成！共 {len(saved_files)} 个AS，总计 {total_size / 1024 / 1024:.2f}MB")

        return saved_files

    def print_summary(self):
        """打印统计摘要"""
        logger.info("\n" + "=" * 60)
        logger.info("提取统计摘要")
        logger.info("=" * 60)
        logger.info(f"输入文件: {self.input_file.name}")
        logger.info(f"总行数: {self.total_lines:,}")
        logger.info(f"已解析行: {self.parsed_lines:,}")
        logger.info(f"提取AS数: {self.as_count}")
        logger.info(f"总前缀数: {self.prefix_count:,}")
        logger.info(f"输出目录: {self.output_dir.absolute()}")
        logger.info("=" * 60)

        # 显示AS排名（前10）
        if self.as_prefixes:
            logger.info("\n前缀数排名（Top 10）:")
            sorted_as = sorted(self.as_prefixes.items(),
                             key=lambda x: len(x[1]), reverse=True)[:10]
            for rank, (as_num, prefixes) in enumerate(sorted_as, 1):
                logger.info(f"  {rank:2d}. AS{as_num}: {len(prefixes):6,} 前缀")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description='从BGP数据文件中提取指定AS的所有前缀',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 提取所有AS的前缀
  python extract_as_prefixes.py rib.20260216.txt

  # 提取指定AS的前缀
  python extract_as_prefixes.py rib.20260216.txt -a 4134 8474

  # 只提取IPv4，使用首个AS（源AS）
  python extract_as_prefixes.py rib.20260216.txt -p first

  # 自定义输出目录
  python extract_as_prefixes.py rib.20260216.txt -o ./output

  # 包括IPv6前缀
  python extract_as_prefixes.py rib.20260216.txt --ipv6
        '''
    )

    parser.add_argument(
        'input_file',
        help='输入的BGP数据文件（mrt2bgpdump格式）'
    )

    parser.add_argument(
        '-a', '--as',
        dest='as_numbers',
        type=int,
        nargs='+',
        help='要提取的AS号列表（不指定则提取所有AS）'
    )

    parser.add_argument(
        '-A', '--as-file',
        dest='as_file',
        type=str,
        help='包含要提取的AS号的文件（每行一个AS号）'
    )

    parser.add_argument(
        '-o', '--output',
        default='./as_prefixes',
        help='输出目录（默认: ./as_prefixes）'
    )

    parser.add_argument(
        '-p', '--position',
        choices=['first', 'last'],
        default='last',
        help='从AS路径中提取哪个AS（first=最前面的AS，last=最后面的AS）'
    )

    parser.add_argument(
        '--ipv6',
        action='store_true',
        help='包括IPv6前缀（默认只提取IPv4）'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='详细输出'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # 创建提取器
        extractor = ASPrefixExtractor(args.input_file, args.output)

        # 提取数据
        if args.as_file:
            try:
                with open(args.as_file, 'r') as f:
                    as_numbers = [int(line.strip()) for line in f if line.strip().isdigit()]
            except Exception as e:
                logger.error(f"读取AS文件失败: {e}")
                sys.exit(1)
        else:
            as_numbers = args.as_numbers
        stats = extractor.extract(
            as_numbers=as_numbers,
            as_position=args.position,
            ipv4_only=not args.ipv6
        )

        if stats:
            # 保存结果
            saved_files = extractor.save_results()

            # 打印摘要
            extractor.print_summary()

            sys.exit(0)
        else:
            logger.error("✗ 未提取到任何数据")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n程序被用户中断")
        sys.exit(130)
    except Exception as e:
        logger.error(f"发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
