from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    app_secret_key: str

    alif_api_key: str
    alif_locale: str = "ru"
    alif_auth_url: str
    alif_client_id: str = "merchant-frontend"
    alif_api_base: str = "https://api-merchant.alif.uz"
    alif_reports_base: str = "https://api-merchant.alif.uz/merchant/excel/excel/v1/reports"

settings = Settings()
