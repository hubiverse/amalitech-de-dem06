from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    # MySQL
    mysql_user: str = "staging_user"
    mysql_password: str = "staging_pass"
    mysql_database: str = "staging_db"
    mysql_host: str = "mysql_staging"
    mysql_port: int = 3306

    # Postgres
    postgres_user: str = "analytics_user"
    postgres_password: str = "analytics_pass"
    postgres_db: str = "analytics_db"
    postgres_host: str = "analytics_db"
    postgres_port: int = 5432


    @computed_field
    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )

    @computed_field
    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


config = Settings()
