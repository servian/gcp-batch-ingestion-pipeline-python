const google = require('googleapis');

const PROJECT_ID = "gcp-batch-pattern";
const TEMPLATE_BUCKET = "servian_melb_practice";
const TEMPLATE_PATH = "templates/majestic_million_template";
const JOB_NAME = "cloud-fn-dataflow";
const FINALIZE = "google.storage.object.finalize";

const getAuthDataflow = (authClient) => {
    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
        authClient = authClient.createScoped([
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/userinfo.email'
        ]);
    }
    return google.dataflow({ version: 'v1b3', auth: authClient });
}

const createDataflowJob = (dataflow, projectId, file) => {
    dataflow.projects.templates.create({
        projectId: projectId,
        resource: {
            parameters: {
                inputFile: `gs://${file.bucket}/${file.name}`
            },
            jobName: `${JOB_NAME}`,
            gcsPath: `gs://${TEMPLATE_BUCKET}/${TEMPLATE_PATH}`
        }
    }, (err, response) => {
        if (err) console.error("Error running dataflow template: ", err);
        console.log("Dataflow template response: ", response);
    });
}

exports.trigger = (event, callback) => {
    const file = event.data;
    const context = event.context;
    if (file.name && context.eventType === FINALIZE) {
        google.auth.getApplicationDefault((err, authClient, projectId) => {
            if (err) throw err;

            const dataflow = getAuthDataflow(authClient);
            createDataflowJob(dataflow, PROJECT_ID, file);
            callback();
        });
    }
};
