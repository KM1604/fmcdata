import tomllib


class SQLDB:
    def __init__(self, cfg_path: dict = "fmcusa_gl_cfg.toml"):
        try:
            with open(cfg_path, "rb") as f:
                self.cfg = tomllib.load(f)
                print(self.cfg.get("readme"))
        except Exception as e:
            print("reading toml failed")
            print(repr(e))
            self.cfg = {}
        """
        # legacy code for reference:
        self.polars_conn = (
            f"mssql://{self.usr}:{self.pw}@"
            f"{self.server}:{self.port}/"
            f"{self.db}?{self.read_flags}"
        )
        self.polars_conn_2 = (
            f"mssql://{self.usr}:{self.pw}@"
            f"{self.server}:{self.port}/"
            f"{self.db}?{self.read_flags_2}"
        )
        """

    def usr(self):
        return self.cfg.get("usr")

    def pw(self):
        return self.cfg.get("pw")

    def server(self):
        return self.cfg.get("server")

    def db(self):
        return self.cfg.get("db")

    def port(self):
        return self.cfg.get("port")

    def driver(self):
        return self.cfg.get("driver")

    def pyodbc_conn(self):
        conn_str = (
            f"DRIVER={self.driver()};"
            f"SERVER={self.server()},{self.port()};"
            f"DATABASE={self.db()};"
            f"UID={self.usr()};"
            f"PWD={self.pw()}"
        )
        return conn_str

    def polars_conn(self, encrypt=None, trust=None):
        """
        returns a connection string based on cfg file read
        depreciated polars_conn_2 uses default values
        depreciated polars_conn uses encrypt='true' and trust='false'

        Keyword Arguments:
        encrypt: str -- true or false (default None)
        trust: str -- true or false (default None)
        """
        if encrypt != None:
            encryption = f"&encrypt={encrypt}"
        else:
            encryption = ""
        if trust != None:
            trusting = f"&trustServerCertificate={trust}"
        else:
            trusting = ""
        conn_str = (
            f"mssql://{self.usr()}:{self.pw()}@"
            + f"{self.server()}:{self.port()}/"
            + f"{self.db()}"
            + f"{self.driver()}"
            + encryption
            + trusting
        )
        return conn_str


def main():
    FMCUSAGL = SQLDB("fmcusa_gl_cfg.toml")
    print(FMCUSAGL.pyodbc_conn())


if __name__ == "__main__":
    main()
