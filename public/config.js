const getApiEndpoint = async () => {
    try {
        // Check if running on localhost (emulator) - this should be the FIRST check
        const isLocalhost = window.location.hostname === 'localhost' || 
                          window.location.hostname === '127.0.0.1';
        
        if (isLocalhost) {
            console.log("Detected localhost environment - using emulator endpoint");
            const emulatorEndpoint = "http://127.0.0.1:5001/pecha-backend-test-3a4d0/us-central1/api";
            return emulatorEndpoint;
        }

        // Only run this logic for non-localhost environments (production)
        const firebaseConfigResponse = await fetch('/__/firebase/init.json', {
            headers: {
                'Accept': 'application/json',
                'Cache-Control': 'no-cache'
            },
            cache: 'no-store'
        });

        if (firebaseConfigResponse.ok) {
            const firebaseConfig = await firebaseConfigResponse.json();
            const projectId = firebaseConfig.projectId;

            console.log(`Detected Firebase project ID: ${projectId}`);

            const config = await loadConfig();
            // Map project IDs to API endpoints - ONLY for production
            if (projectId === 'pecha-backend') {
                console.log("Using production environment based on project ID");
                return config.PROD_API;
            } else if (projectId === 'pecha-backend-dev') {
                console.log("Using development environment based on project ID");
                return config.DEV_API;
            } else if (projectId === 'pecha-backend-test-3a4d0') {
                console.log("Using test environment based on project ID");
                return config.TEST_API;
            }
        } else {
            throw new Error("Failed to fetch Firebase init file");
        }

    } catch (firebaseError) {
        console.warn("Failed to fetch Firebase init file:", firebaseError);
        return null;
    }
};

const loadConfig = async () => {
    try {
        const response = await fetch('/config.json');
        const config = await response.json();
        return config;
    } catch (error) {
        console.error("Failed to load config:", error);
        return null;
    }
}
