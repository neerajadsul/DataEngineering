"""Connect and upload data to the specified databased."""
import yaml


class DatabaseConnector:
    def read_db_creds(self, creds_file='db_creds.yaml'):
        creds = None
        try:
            with open(creds_file) as f:
                creds = yaml.load(f, yaml.Loader)
        except FileNotFoundError:
            print(f'File not found, {creds_file}')
        except IOError:
            print(f"Found but could not access {creds_file}")
        return creds


if __name__ == "__main__":
    pass
