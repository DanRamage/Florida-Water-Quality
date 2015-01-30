import logging.config
from datetime import datetime
from xmrgFile import xmrgCleanup



def main():
  src_dir = "/home/xeniaprod/tmp/dhec/xmrgdata"
  dst_dir = "/home/xeniaprod/tmp/dhec/xmrgarchive/tmp"

  logging.config.fileConfig("/home/xeniaprod/scripts/Florida-Water-Quality/config/floridaXMRGProcessing.conf")
  logger = logging.getLogger("xmrg_cleanup_logger")

  keep_time = datetime.strptime("2015-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
  x_cleanup = xmrgCleanup(src_dir, dst_dir)
  x_cleanup.organizeFilesIntoDirectories(keep_time)
  return


if __name__ == "__main__":
  main()
