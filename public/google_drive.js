// Load configuration asynchronously
let config;
async function loadConfiguration() {
    console.log("inside config")
    config = await loadConfig();
    return config;
}

// Initialize configuration loading
const configPromise = loadConfiguration();

// Token management functions
const TOKEN_STORAGE_KEY = 'openPechaGoogleAuthToken';

// Store token in localStorage with expiration
function storeToken(token) {
    if (!token) return;
    
    const tokenData = {
        token: token,
        expiry: new Date().getTime() + (3600 * 1000) // 1 hour expiration
    };
    
    try {
        localStorage.setItem(TOKEN_STORAGE_KEY, JSON.stringify(tokenData));
        console.log('Token stored in localStorage');
    } catch (error) {
        console.error('Error storing token:', error);
    }
}

// Retrieve token from localStorage
function retrieveStoredToken() {
    try {
        const tokenData = localStorage.getItem(TOKEN_STORAGE_KEY);
        if (!tokenData) return null;
        
        const parsedData = JSON.parse(tokenData);
        
        // Check if token is expired
        if (new Date().getTime() > parsedData.expiry) {
            console.log('Stored token is expired');
            localStorage.removeItem(TOKEN_STORAGE_KEY);
            return null;
        }
        
        return parsedData.token;
    } catch (error) {
        console.error('Error retrieving token:', error);
        return null;
    }
}

let tokenClient;
function gisLoaded() {
    console.log("gis loaded");
    configPromise.then(config => {
        tokenClient = google.accounts.oauth2.initTokenClient({
            client_id: config.CLIENT_ID,
            scope: config.SCOPES,
            callback: (tokenResponse) => {
                // Store token when received from Google
                if (tokenResponse && !tokenResponse.error) {
                    storeToken(gapi.client.getToken());
                }
            },
        });
        
        // Check if we have a stored token and set it
        const storedToken = retrieveStoredToken();
        if (storedToken) {
            console.log('Using stored token');
            gapi.client.setToken(storedToken);
        }
    });
}

function gapiLoaded() {
    gapi.load('client', initializeGapiClient);
}

async function initializeGapiClient() {
    await configPromise;
    await gapi.client.init({
        apiKey: config.API_KEY,
        discoveryDocs: [config.DISCOVERY_DOC],
    });
    
    // Set token if available in storage
    const storedToken = retrieveStoredToken();
    if (storedToken) {
        gapi.client.setToken(storedToken);
    }
}

async function downloadDocSession(docId) {
    try {
        const response = await gapi.client.drive.files.export({
            fileId: docId,
            mimeType: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        });

        if (!response.body) {
            throw new Error("No file content received");
        }

        const charArray = new Array(response.body.length);
        for (let i = 0; i < response.body.length; i++) {
            charArray[i] = response.body.charCodeAt(i);
        }
        const typedArray = new Uint8Array(charArray);

        return new Blob([typedArray], {type: response.headers['Content-Type']});
    } catch (err) {
        console.error('Error downloading file:', err);
        alert('Failed to download file: ' + err.message);
    }
}

async function downloadDoc(docId) {
    return new Promise((resolve, reject) => {
        // Set up callback for token client
        tokenClient.callback = async (resp) => {
            if (resp.error !== undefined) {
                reject(resp);
                return;
            }
            try {
                // Store the token after successful authentication
                storeToken(gapi.client.getToken());
                const result = await downloadDocSession(docId);
                resolve(result);
            } catch (error) {
                reject(error);
            }
        };

        // Check for stored token first
        const storedToken = retrieveStoredToken();
        if (storedToken) {
            // Set the token and try to use it
            gapi.client.setToken(storedToken);
            downloadDocSession(docId)
                .then(resolve)
                .catch(error => {
                    console.error('Error with stored token:', error);
                    // If token is invalid, request a new one
                    if (gapi.client.getToken() === null) {
                        tokenClient.requestAccessToken({ prompt: 'consent' });
                    } else {
                        // Try silent token refresh
                        tokenClient.requestAccessToken({ prompt: '' });
                    }
                });
        } else if (gapi.client.getToken() === null) {
            // No stored token and no current token, request with consent
            tokenClient.requestAccessToken({ prompt: 'consent' });
        } else {
            // We have a current token but no stored token, store it and use it
            storeToken(gapi.client.getToken());
            downloadDocSession(docId)
                .then(resolve)
                .catch(error => {
                    // If token is invalid, request a new one
                    tokenClient.requestAccessToken({ prompt: '' });
                });
        }
    });
}