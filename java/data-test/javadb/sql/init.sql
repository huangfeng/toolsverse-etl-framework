CREATE TABLE source 
(
    source_num INT NOT NULL,
    description VARCHAR(40) NOT NULL,
    PRIMARY KEY (source_num)
);

CREATE TABLE data 
(
    data_num  INT NOT NULL,
    source_num INT NOT NULL, 
    data VARCHAR(255),
    PRIMARY KEY (data_num),
    FOREIGN KEY (source_num) REFERENCES  source (source_num) 
);


create view data_source as
select source.description,
         data.data
from data, source
where source.source_num = data.source_num

