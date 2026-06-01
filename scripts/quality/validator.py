import logging
from pathlib import Path
from soda.scan import Scan

logger = logging.getLogger(__name__)


def run_quality_checks(
    data_source_name: str, checks_path: Path, connection_config: dict
) -> bool:
    """Run Soda Core quality checks against a data source.

    Returns True if all checks pass, False otherwise.
    """
    scan = Scan()
    scan.set_data_source_name(data_source_name)
    scan.add_configuration_yaml_str(f"""
        data_source {data_source_name}:
          type: postgres
          host: {connection_config['host']}
          port: {connection_config['port']}
          username: {connection_config['user']}
          password: {connection_config['password']}
          database: {connection_config['database']}
    """)
    scan.add_sodacl_yaml_file(str(checks_path))

    exit_code = scan.execute()

    if exit_code != 0:
        logger.error(f"Quality checks failed with exit code: {exit_code}")
        logger.error(scan.get_logs_text())
        return False

    logger.info("All quality checks passed.")
    return True
