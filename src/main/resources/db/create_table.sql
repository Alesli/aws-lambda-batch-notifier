create table image(
    id               BIGINT          PRIMARY KEY AUTO_INCREMENT,
    name             VARCHAR(20),
    image_size       BIGINT,
    file_extension   VARCHAR(40) ,
    last_update_date       TIMESTAMP       DEFAULT NOW()
--    bitmap_id        VARCHAR(128)
);