CREATE TABLE MediaItems(
    MID number(9,0) NOT NULL,
    TITLE varchar2(200) NOT NULL,
    PROD_YEAR number(4) NOT NULL,
    TITLE_LENGTH number(4) NOT NULL,
    CONSTRAINT mid_pk PRIMARY KEY(MID)
);

CREATE TABLE Similarity(
    MID1 number(9,0) NOT NULL,
    MID2 number(9,0) NOT NULL,
    SIMILARITY float,
    CONSTRAINT mid1_mid2_pk PRIMARY KEY (MID1, MID2),
    CONSTRAINT fk_mediaitems1 FOREIGN KEY (MID1) REFERENCES MediaItems(MID),
    CONSTRAINT fk_mediaitems2 FOREIGN KEY (MID2) REFERENCES MediaItems(MID)
);

CREATE OR REPLACE TRIGGER AutoIncrement
    BEFORE INSERT ON MediaItems
    FOR EACH ROW
    DECLARE
        v_mid_index NUMBER;
    BEGIN
        SELECT MAX(MID) INTO v_mid_index FROM MediaItems;
        IF v_mid_index IS NULL THEN
            :new.MID := 0;
        ELSE
            :new.MID := v_mid_index + 1;
        END IF;
        :new.TITLE_LENGTH := LENGTH(:new.TITLE);
END;

CREATE OR REPLACE FUNCTION MaximalDistance
    RETURN NUMBER IS 
        v_min_prod_year NUMBER;
        v_max_prod_year NUMBER;
BEGIN
    SELECT MIN(PROD_YEAR) INTO v_min_prod_year FROM MediaItems;
    SELECT MAX(PROD_YEAR) INTO v_max_prod_year FROM MediaItems;
    RETURN POWER((v_max_prod_year - v_min_prod_year), 2);
END MaximalDistance;

CREATE OR REPLACE FUNCTION SimCalculation(mid1 IN NUMBER, mid2 IN NUMBER, maximal_distance IN NUMBER)
    RETURN FLOAT IS 
        v_two_items_distance NUMBER;
        v_mid1_prod_year NUMBER;
        v_mid2_prod_year NUMBER;
BEGIN
    SELECT PROD_YEAR INTO v_mid1_prod_year FROM MediaItems WHERE MID=mid1;
    SELECT PROD_YEAR INTO v_mid2_prod_year FROM MediaItems WHERE MID=mid2;
    v_two_items_distance := POWER((v_mid1_prod_year - v_mid2_prod_year), 2);
    RETURN (1 - (v_two_items_distance/maximal_distance));
END SimCalculation;
    
