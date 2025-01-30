const CLIENT_ID = '210223691571-ul4cfmq97eoudb7b8ujg4av2u271l2gm.apps.googleusercontent.com';
const API_KEY = 'AIzaSyDO2sGIHrTBwA80-igeT60To6OIqacaqpY';
const SCOPES = 'https://www.googleapis.com/auth/drive.readonly';
const DISCOVERY_DOC = 'https://www.googleapis.com/discovery/v1/apis/drive/v3/rest';

let tokenClient;

function gisLoaded() {
    console.log("gis loaded")
    tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: CLIENT_ID,
        scope: SCOPES,
        callback: '',
    });
}

function gapiLoaded() {
    gapi.load('client', initializeGapiClient);
}

async function initializeGapiClient() {
    await gapi.client.init({
        apiKey: API_KEY,
        discoveryDocs: [DISCOVERY_DOC],
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