-- StackOverflow PAC Links Configuration
-- Execute this after the tables are created and loaded with data
-- PAC_LINKs define foreign key relationships for PAC compilation
--
-- Link graph (no cycles):
--   Users (PU)
--     ├── Badges.UserId -> Users.Id
--     ├── Posts.OwnerUserId -> Users.Id
--     │     └── PostLinks.PostId -> Posts.Id
--     ├── Comments.UserId -> Users.Id
--     ├── PostHistory.UserId -> Users.Id
--     └── Votes.UserId -> Users.Id

-- Mark Users as the privacy unit
ALTER TABLE Users SET PU;

-- Protected columns in Users table
ALTER PU TABLE Users ADD PROTECTED (Id);
ALTER PU TABLE Users ADD PROTECTED (DisplayName);
ALTER PU TABLE Users ADD PROTECTED (WebsiteUrl);
ALTER PU TABLE Users ADD PROTECTED (Location);
ALTER PU TABLE Users ADD PROTECTED (AboutMe);
ALTER PU TABLE Users ADD PROTECTED (ProfileImageUrl);
ALTER PU TABLE Users ADD PROTECTED (AccountId);

-- Badges -> Users link
ALTER PU TABLE Badges ADD PAC_LINK (UserId) REFERENCES Users(Id);

-- Posts -> Users link
ALTER PU TABLE Posts ADD PAC_LINK (OwnerUserId) REFERENCES Users(Id);

-- Comments -> Users link
ALTER PU TABLE Comments ADD PAC_LINK (UserId) REFERENCES Users(Id);

-- PostHistory -> Users link
ALTER PU TABLE PostHistory ADD PAC_LINK (UserId) REFERENCES Users(Id);

-- Votes -> Users link
ALTER PU TABLE Votes ADD PAC_LINK (UserId) REFERENCES Users(Id);

-- PostLinks -> Posts link (transitive: PostLinks -> Posts -> Users)
ALTER PU TABLE PostLinks ADD PAC_LINK (PostId) REFERENCES Posts(Id);
