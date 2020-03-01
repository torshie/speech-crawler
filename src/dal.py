import sqlite3
import os.path


class DataAccessLayer:
    STATUS_UNKNOWN_ERROR = 1
    STATUS_NEW = 2
    STATUS_SOURCE_ERROR = 3
    STATUS_DONE = 5

    STATUS_ALIGNED = 6
    STATUS_BAD_CONTENT = 7
    STATUS_GOOD_CONTENT = 8

    def __init__(self, dbfile):
        if not os.path.isfile(dbfile) and not os.path.islink(dbfile):
            self.__connection = self.__create_db(dbfile)
        else:
            self.__connection = sqlite3.connect(dbfile)

    def add_search_query(self, query):
        self.__connection.execute(
            "INSERT INTO search (query, status) VALUES (?, ?)",
            (query, self.STATUS_NEW))
        self.__connection.commit()

    def fetch_new_queries(self):
        cursor = self.__connection.cursor()
        cursor.execute('SELECT query, wip FROM search WHERE status = ? ORDER BY query ASC',
            [self.STATUS_NEW])
        return cursor.fetchall()

    def set_query_wip(self, query, wip):
        cursor = self.__connection.cursor()
        cursor.execute('UPDATE search SET wip = ? WHERE query = ?',
            [wip, query])
        assert cursor.rowcount == 1
        self.__connection.commit()

    def set_query_done(self, query):
        cursor = self.__connection.cursor()
        cursor.execute('UPDATE search SET status = ? WHERE query = ?',
            [self.STATUS_DONE, query])
        assert cursor.rowcount == 1
        self.__connection.commit()

    def add_channel(self, channel_id, size):
        self.__connection.execute(
            'INSERT INTO channel (channel_id, size, status) VALUES (?, ?, ?)',
            [channel_id, size, self.STATUS_NEW])
        self.__connection.commit()

    def fetch_good_channels(self):
        cursor = self.__connection.cursor()
        cursor.execute('SELECT channel_id, wip FROM channel WHERE status = ? ORDER BY create_time ASC',
            [self.STATUS_NEW])
        return cursor.fetchall()

    def set_channel_wip(self, channel_id, wip):
        cursor = self.__connection.cursor()
        cursor.execute('UPDATE channel SET wip = ? WHERE channel_id = ?',
            [wip, channel_id])
        assert cursor.rowcount == 1
        self.__connection.commit()

    def set_channel_done(self, channel_id):
        cursor = self.__connection.cursor()
        cursor.execute('UPDATE channel SET status = ? WHERE channel_id = ?',
            [self.STATUS_DONE, channel_id])
        assert cursor.rowcount == 1
        self.__connection.commit()

    def add_video(self, video_id, channel_id):
        cursor = self.__connection.cursor()
        try:
            cursor.execute('INSERT INTO video (video_id, channel_id, status) VALUES (?, ?, ?)',
                [video_id, channel_id, self.STATUS_NEW])
        except sqlite3.IntegrityError:
            pass
        self.__connection.commit()

    def fetch_new_videos(self):
        cursor = self.__connection.cursor()
        cursor.execute('SELECT video_id, channel_id FROM video WHERE status = ? SORT BY create_time ASC')
        return cursor.fetchall()

    def set_video_status(self, video_id, status):
        cursor = self.__connection.cursor()
        cursor.execute('UPDATE video SET status = ? WHERE video_id = ?',
            [video_id, status])
        assert cursor.rowcount == 1
        self.__connection.commit()

    def __create_db(self, filename):
        connection = sqlite3.connect(filename)

        cursor = connection.cursor()
        cursor.execute("""CREATE TABLE search (
            query VARCHAR(255) PRIMARY KEY,
            status INT NOT NULL,
            wip TEXT,
            create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""")

        cursor.execute("""CREATE TABLE channel (
            channel_id VARCHAR(255) PRIMARY KEY,
            status INT NOT NULL,
            wip TEXT,
            size INT,
            num_checked INT NOT NULL DEFAULT 0,
            num_valid INT NOT NULL DEFAULT 0,
            create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""")

        cursor.execute("""CREATE TABLE video (
            video_id VARCHAR(255) PRIMARY KEY,
            status INT NOT NULL,
            channel_id VARCHAR(255),
            file VARCHAR(255),
            length INT,
            publish_time DATETIME,
            create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""")

        cursor.execute("""CREATE TABLE subtitle (
            subtitle_id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id VARCHAR(255) NOT NULL,
            status INT NOT NULL,
            begin_time INT NOT NULL,
            end_time INT NOT NULL,
            content TEXT NOT NULL,
            create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (video_id) REFERENCES video(video_id)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")

        connection.commit()

        return connection
