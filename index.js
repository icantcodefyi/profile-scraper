const axios = require('axios');
const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { stringify } = require('csv-stringify/sync');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

const GITHUB_API_BASE_URL = 'https://api.github.com';
const GITHUB_TOKEN = "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";
const MAX_CONCURRENT_REQUESTS = 16;
const RETRY_DELAY = 10000;

if (isMainThread) {
    async function scrapeGithubProfiles(inputFile, outputFile) {
        try {
            // Read and parse the input CSV file
            const fileContent = fs.readFileSync(inputFile, 'utf8');
            const records = parse(fileContent, { columns: true, skip_empty_lines: true });

            // Extract unique usernames
            const usernames = [...new Set(records.map(record => record.contributor_login))];

            // Prepare output file with headers
            const headers = [
                'username', 'name', 'avatarUrl', 'bio', 'company', 'location', 'email',
                'website', 'twitter', 'followersCount', 'followingCount', 'publicRepos',
                'publicGists', 'createdAt', 'updatedAt', 'repoName', 'repoDescription',
                'repoUrl', 'repoStars', 'repoForks', 'repoWatchers', 'repoLanguage', 'repoCreatedAt',
                'repoUpdatedAt', 'repoPushedAt', 'repoSize', 'repoOpenIssues', 'repoLicense',
                'repoDefaultBranch', 'repoCommitCount', 'repoLatestCommitSha', 'repoLatestCommitMessage',
                'repoLatestCommitAuthor', 'repoLatestCommitDate'
            ];
            fs.writeFileSync(outputFile, stringify([headers]));

            // Create a queue of usernames
            const queue = usernames.slice();
            const numCPUs = os.cpus().length;
            const numWorkers = Math.min(numCPUs, MAX_CONCURRENT_REQUESTS);

            console.log(`Starting ${numWorkers} worker threads...`);

            // Create worker threads
            const workers = new Array(numWorkers).fill(null).map(() =>
                new Worker(__filename, { workerData: { GITHUB_TOKEN } })
            );

            // Process queue with workers
            let completedCount = 0;
            const totalCount = queue.length;

            function assignTaskToWorker(worker) {
                if (queue.length > 0) {
                    const username = queue.pop();
                    worker.postMessage(username);
                } else if (completedCount === totalCount) {
                    workers.forEach(w => w.terminate());
                    console.log('All tasks completed.');
                }
            }

            workers.forEach(worker => {
                worker.on('message', (results) => {
                    if (results.error) {
                        console.error(`Error processing ${results.username}: ${results.error}`);
                        // Write error to a separate error log file
                        fs.appendFileSync('error_log.txt', `${results.username}: ${results.error}\n`);
                    } else {
                        fs.appendFileSync(outputFile, stringify(results));
                        completedCount++;
                        console.log(`Processed ${completedCount}/${totalCount} users`);
                    }
                    assignTaskToWorker(worker);
                });
                worker.on('error', (error) => {
                    console.error('Worker error:', error);
                });

                worker.on('exit', (code) => {
                    if (code !== 0) {
                        console.error(`Worker stopped with exit code ${code}`);
                    }
                });

                // Start the worker with its first task
                assignTaskToWorker(worker);
            });

        } catch (error) {
            console.error('Error processing input file:', error.message);
        }
    }

    // Check if GITHUB_TOKEN is set
    if (!GITHUB_TOKEN) {
        console.error('GITHUB_TOKEN is not set. Please set it as an environment variable.');
        process.exit(1);
    }

    // Get input and output filenames from command line arguments
    const inputFile = process.argv[2];
    const outputFile = process.argv[3] || 'github_profiles_and_repos.csv';

    if (!inputFile) {
        console.error('Please provide an input CSV file as a command line argument.');
        console.error('Usage: node script_name.js <input_file.csv> [output_file.csv]');
        process.exit(1);
    }

    scrapeGithubProfiles(inputFile, outputFile);

} else {
    // This code runs in worker threads
    const { GITHUB_TOKEN } = workerData;

    async function fetchWithRetry(url, headers, retries = 3) {
        try {
            const response = await axios.get(url, { headers });
            return response.data;
        } catch (error) {
            if (error.response && error.response.status === 403) {
                console.warn(`Rate limit exceeded. Retrying in ${RETRY_DELAY / 1000} seconds...`);
                if (retries > 0) {
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                    return fetchWithRetry(url, headers, retries - 1);
                } else {
                    throw new Error('Rate limit exceeded. Max retries reached.');
                }
            } else if (error.response && error.response.status === 404) {
                console.warn(`Resource not found: ${url}`);
                return null;
            } else {
                throw error;
            }
        }
    }

    async function fetchUserDataAndRepos(username) {
        const headers = {
            'Authorization': `token ${GITHUB_TOKEN}`,
            'Accept': 'application/vnd.github.v3+json'
        };

        try {
            // Fetch user data
            const user = await fetchWithRetry(`${GITHUB_API_BASE_URL}/users/${username}`, headers);
            if (!user) {
                return [{ username, error: 'User not found' }];
            }

            // Fetch user's repositories
            const repos = await fetchWithRetry(`${GITHUB_API_BASE_URL}/users/${username}/repos`, headers);
            if (!repos) {
                return [{ username, error: 'Unable to fetch repositories' }];
            }

            const results = [];

            // Prepare user data (will be repeated for each repo)
            const userData = {
                username: user.login,
                name: user.name || 'N/A',
                avatarUrl: user.avatar_url,
                bio: user.bio || 'N/A',
                company: user.company || 'N/A',
                location: user.location || 'N/A',
                email: user.email || 'N/A',
                website: user.blog || 'N/A',
                twitter: user.twitter_username || 'N/A',
                followersCount: user.followers,
                followingCount: user.following,
                publicRepos: user.public_repos,
                publicGists: user.public_gists,
                createdAt: user.created_at,
                updatedAt: user.updated_at,
            };

            // If user has no repos, return user data with empty repo fields
            if (repos.length === 0) {
                results.push({
                    ...userData,
                    repoName: 'N/A',
                    repoDescription: 'N/A',
                    repoUrl: 'N/A',
                    repoStars: 'N/A',
                    repoForks: 'N/A',
                    repoWatchers: 'N/A',
                    repoLanguage: 'N/A',
                    repoCreatedAt: 'N/A',
                    repoUpdatedAt: 'N/A',
                    repoPushedAt: 'N/A',
                    repoSize: 'N/A',
                    repoOpenIssues: 'N/A',
                    repoLicense: 'N/A',
                    repoDefaultBranch: 'N/A',
                    repoCommitCount: 'N/A',
                    repoLatestCommitSha: 'N/A',
                    repoLatestCommitMessage: 'N/A',
                    repoLatestCommitAuthor: 'N/A',
                    repoLatestCommitDate: 'N/A'
                });
            } else {
                // Combine user data with each repo's data
                for (const repo of repos) {
                    // Fetch commit information
                    const commits = await fetchWithRetry(`${GITHUB_API_BASE_URL}/repos/${username}/${repo.name}/commits`, headers);
                    const latestCommit = commits && commits[0] ? commits[0] : {};

                    results.push({
                        ...userData,
                        repoName: repo.name,
                        repoDescription: repo.description || 'N/A',
                        repoUrl: repo.html_url,
                        repoStars: repo.stargazers_count,
                        repoForks: repo.forks_count,
                        repoWatchers: repo.watchers_count,
                        repoLanguage: repo.language || 'N/A',
                        repoCreatedAt: repo.created_at,
                        repoUpdatedAt: repo.updated_at,
                        repoPushedAt: repo.pushed_at,
                        repoSize: repo.size,
                        repoOpenIssues: repo.open_issues_count,
                        repoLicense: repo.license ? repo.license.name : 'N/A',
                        repoDefaultBranch: repo.default_branch,
                        repoCommitCount: commits ? commits.length : 'N/A',
                        repoLatestCommitSha: latestCommit.sha || 'N/A',
                        repoLatestCommitMessage: latestCommit.commit ? latestCommit.commit.message : 'N/A',
                        repoLatestCommitAuthor: latestCommit.commit ? latestCommit.commit.author.name : 'N/A',
                        repoLatestCommitDate: latestCommit.commit ? latestCommit.commit.author.date : 'N/A'
                    });
                }
            }

            return results;
        } catch (error) {
            return [{ username, error: error.message }];
        }
    }

    parentPort.on('message', async (username) => {
        const results = await fetchUserDataAndRepos(username);
        parentPort.postMessage(results);
    });
}