PRAGMA clear_pac_metadata;
ALTER TABLE hits SET PU;
ALTER PU TABLE hits ADD PROTECTED (UserID, ClientIP);
