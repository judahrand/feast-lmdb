from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

LMDB_CONFIG = {"type": "feast_lmdb.lmdb_online_store.LMDBOnlineStore"}

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(online_store=LMDB_CONFIG),
]