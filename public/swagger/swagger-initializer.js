window.onload = function() {
  //<editor-fold desc="Changeable Configuration Block">

  // Fetch the config.json file to get the API endpoint
  fetch('../config.json')
    .then(response => response.json())
    .then(config => {
      // Construct the OpenAPI schema URL by appending the path to the API endpoint
      const schemaUrl = `${config.apiEndpoint}/schema/openapi`;
      console.log("schemaUrl", schemaUrl);
      
      // Initialize Swagger UI with the dynamic URL
      window.ui = SwaggerUIBundle({
        url: schemaUrl,
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      });
    })
    .catch(error => {
      console.error('Error loading config.json:', error);
    });

  //</editor-fold>
};
