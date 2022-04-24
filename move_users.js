import { createClient } from '@supabase/supabase-js';
import Airtable from 'airtable';

var airtable = new Airtable({ apiKey: process.env.AIRTABLE_KEY }).base(process.env.AIRTABLE_BASE_ID);
const supabase = createClient(process.env.SUPABASE_DB_URL, process.env.SUPABASE_KEY);

const retrieveLastSync = async () => {
    const timestamp = await airtable('users').find(process.env.AIRTABLE_TIMESTAMP_RECORD_ID);
    console.log('Retrieved timestamp is ' + timestamp.fields.timestamp);
    return timestamp.fields.timestamp;
};

const updateLastSync = async () => {
    const now = new Date().toISOString();
    await airtable('timestamp').update([
        {
            id: process.env.AIRTABLE_TIMESTAMP_RECORD_ID,
            fields: {
                timestamp: now,
            },
        },
    ]);
};

const writeToAirtable = async (lastSyncTimestamp) => {
    const { data, error } = await supabase
        .from('users')
        .select('first_name,last_name')
        .gt('created_at', lastSyncTimestamp);

    if (error) {
        console.log(error);
        process.exit();
    }

    if (data.length == 0) {
        return [];
    }

    var rows = [];
    for (var row = 0; row < data.length; row++) {
        rows.push({ fields: data[row] });
    }

    airtable('users').create(rows, function (err, records) {
        if (err) {
            console.error(err);
            process.exit();
        }
    });

    return data;
};

export default async function (params) {
    const lastSyncTimestamp = await retrieveLastSync();
    const data = await writeToAirtable(lastSyncTimestamp);
    await updateLastSync();
    return data;
}
