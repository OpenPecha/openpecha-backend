const getApiEndpoint = async () => {
    try {
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
            // Map project IDs to API endpoints
            if (projectId === 'pecha-backend') {
                console.log("Using production environment based on project ID");
                return config.PROD_API;
            } else if (projectId === 'pecha-backend-dev') {
                console.log("Using development environment based on project ID");
                return config.DEV_API;
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
