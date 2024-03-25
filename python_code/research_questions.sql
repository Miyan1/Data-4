-- [S1] What role do date parameters (days, weeks, months, season) have on the number of caches?
SELECT
  DayOfTheWeek,
  COUNT(*) AS NumberOfCachesFound
FROM
  TreasureFound_Fact
JOIN
  Date_Dim ON TreasureFound_Fact.DateSK = Date_Dim.DateSK
GROUP BY
  DayOfTheWeek
ORDER BY
  DayOfTheWeek;

SELECT
  Week,
  COUNT(*) AS NumberOfCachesFound
FROM
  TreasureFound_Fact
JOIN
  Date_Dim ON TreasureFound_Fact.DateSK = Date_Dim.DateSK
GROUP BY
  Week
ORDER BY
  Week;

SELECT
  Month,
  COUNT(*) AS NumberOfCachesFound
FROM
  TreasureFound_Fact
JOIN
  Date_Dim ON TreasureFound_Fact.DateSK = Date_Dim.DateSK
GROUP BY
  Month
ORDER BY
  Month;

SELECT
  Season,
  COUNT(*) AS NumberOfCachesFound
FROM
  TreasureFound_Fact
JOIN
  Date_Dim ON TreasureFound_Fact.DateSK = Date_Dim.DateSK
GROUP BY
  Season
ORDER BY
  Season;


-- [S2] How does the type of user affect the duration of the treasure hunt? Does a beginner take longer?
SELECT
    u.ExperienceLevel,
    AVG(CAST(t.SearchTime AS FLOAT)) AS AvgSearchTime
FROM
    TreasureFound_Fact t
INNER JOIN
    UserDim u ON t.UserSK = u.UserSK
GROUP BY
    u.ExperienceLevel
ORDER BY
    AvgSearchTime DESC;


-- [S2] On average, do users find the cache faster in the rain?
SELECT
    r.weather,
    AVG(CAST(t.SearchTime AS FLOAT)) AS AvgSearchTime
FROM
    TreasureFound_Fact t
INNER JOIN
    Rain_Dim r ON t.WeatherID = r.id
GROUP BY
    r.weather;

-- [S2] Are novice users on average looking for caches with more stages?
SELECT
    UD.ExperienceLevel,
    AVG(CAST(TFF.TotalStages AS FLOAT)) AS AvgStages
FROM
    UserDim AS UD
JOIN
    TreasureFound_Fact AS TFF ON UD.UserSK = TFF.UserSK
WHERE
    UD.ExperienceLevel = 'Amateur'
GROUP BY
    UD.ExperienceLevel;


-- [S1] On average, are fewer caches searched in more difficult terrain when it rains?
SELECT
  AVG(CASE WHEN Rain_Dim.weather = 'Rain' THEN 1 ELSE 0 END) AS AvgSearchesInRain,
  TreasureTypeDim.terrain,
  TreasureTypeDim.difficulty
FROM
  TreasureFound_Fact
JOIN
  TreasureTypeDim ON TreasureFound_Fact.TreasureID = TreasureTypeDim.id
JOIN
  Rain_Dim ON TreasureFound_Fact.WeatherID = Rain_Dim.id
GROUP BY
  TreasureTypeDim.terrain,
  TreasureTypeDim.difficulty;

-- [S1] Are more difficult caches done on weekends?
SELECT
  Date_Dim.DayOfTheWeek,
  AVG(TreasureTypeDim.difficulty) AS AvgDifficulty
FROM
  TreasureFound_Fact
JOIN
  TreasureTypeDim ON TreasureFound_Fact.TreasureID = TreasureTypeDim.id
JOIN
  Date_Dim ON TreasureFound_Fact.DateSK = Date_Dim.DateSK
GROUP BY
  Date_Dim.DayOfTheWeek
ORDER BY
  Date_Dim.DayOfTheWeek;

-- EXTRA RESEARCH QUESTIONS
-- [S1] Do treasures with higher difficulty also contain more stages, on average?
SELECT
  AVG(TotalStages), TreasureTypeDim.difficulty
FROM
  TreasureFound_Fact
JOIN
  TreasureTypeDim ON TreasureFound_Fact.TreasureID = TreasureTypeDim.id GROUP by TreasureTypeDim.difficulty;

-- [S1] Do longer (more detailed) descriptions allow users to find treasure quicker, on average?
SELECT
  AVG(TreasureFound_Fact.SearchTime) AS AvgSearchTime,
  CASE 
    WHEN LEN(TreasureFound_Fact.DescriptionLength) < (choose a value) THEN 'Short'
    WHEN LEN(TreasureFound_Fact.DescriptionLength) BETWEEN (choose a value) AND (choose a value) THEN 'Medium'
    ELSE 'Long'
  END AS DescriptionLengthCategory
FROM
  TreasureFound_Fact
JOIN
  TreasureTypeDim ON TreasureFound_Fact.TreasureID = TreasureTypeDim.id
GROUP BY
  TreasureFound_Fact.DescriptionLength;

-- [S2] Does the size of the container influence the difficulty of found treasures?
SELECT
    TTD.size,
    AVG(TTD.difficulty) as AverageDifficulty
FROM
    TreasureTypeDim TTD
JOIN
    TreasureFound_Fact TFF ON TTD.id = TFF.TreasureID
GROUP BY
    TTD.size;

-- [S2] Does rainfall influence the likelihood of finding a treasure?
--  We deduce by comparing the proportion of found treasures during rain x no rain
SELECT
    RD.weather,
    COUNT(TFF.FactID) as NumberOfFinds
FROM
    RainDim RD
JOIN
    TreasureFound_Fact TFF ON RD.id = TFF.WeatherID
GROUP BY
    RD.weather;
