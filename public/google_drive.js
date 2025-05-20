// Load configuration asynchronously
let config;
async function loadConfiguration() {
    console.log("inside config")
    config = await loadConfig();
    return config;
}

// Initialize configuration loading
const configPromise = loadConfiguration();

let tokenClient;
function gisLoaded() {
    console.log("gis loaded");
    configPromise.then(config => {
        tokenClient = google.accounts.oauth2.initTokenClient({
            client_id: config.CLIENT_ID,
            scope: config.SCOPES,
            callback: '',
        });
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
        tokenClient.callback = async (resp) => {
            if (resp.error !== undefined) {
                reject(resp);
                return;
            }
            try {
                const result = await downloadDocSession(docId);
                resolve(result);
            } catch (error) {
                reject(error);
            }
        };

        if (gapi.client.getToken() === null) {
            tokenClient.requestAccessToken({ prompt: 'consent' });
        } else {
            tokenClient.requestAccessToken({ prompt: '' });
        }
    });
}