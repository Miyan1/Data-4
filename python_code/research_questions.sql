-- [S1] What role do date parameters (days, weeks, months, season) have on the number of caches?



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
-- [S1] Are more difficult caches done on weekends?


-- EXTRA RESEARCH QUESTIONS
-- [S1] Do users searching for treasures in their home city find them more often, on average, than users from different locations?
-- [S1] Do longer (more detailed) descriptions allow users to find treasure quicker, on average?

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
