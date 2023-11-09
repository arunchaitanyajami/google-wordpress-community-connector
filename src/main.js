/* eslint-disable prefer-rest-params */
/* eslint-disable prefer-spread */

var cc = DataStudioApp.createCommunityConnector();

function isNumeric(value) {
    return /^-?\d+$/.test(value);
}

function isLatitude(value) {
    return typeof value === 'number' && value >= -90 && value <= 90;
}

function isLongitude(value) {
    return typeof value === 'number' && value >= -180 && value <= 180;
}

function isLatitudeAndLongitude(value) {
    var parts = value.split(',')
    if (isLatitude(parts[0]) && isLongitude(parts[1])) {
        return true;
    } else {
        return false;
    }
}

/**
 * Validate Url.
 * @returns {boolean}
 * @param str
 */
function validURL(str) {
    return str.match(/^https?:\/\/.+$/g)
}

/**
 * Throws and logs script exceptions.
 *
 * @param {String} message The exception message
 */
function sendUserError(message) {
    cc.newUserError()
        .setText(message)
        .throwException();
}

/**
 * Gets UrlFetch response and parses JSON
 *
 * @param   {string} url  The URL to get the data from
 * @returns {Object}      The response object
 */
function fetchJSON(url) {
    try {
        var response = UrlFetchApp.fetch(url);
    } catch (e) {
        sendUserError('"' + url + '" returned an error:' + e);
    }

    try {
        var content = JSON.parse(response);
    } catch (e) {
        sendUserError('Invalid JSON format. ' + e);
    }

    return content;
}

/**
 * Fetches data. Either by calling getCachedData or fetchJSON, depending on the cache configuration parameter.
 *
 * @param   {String}  url   The URL to get the data from
 *
 * @returns {Object}        The response object
 */
function fetchData(url) {
    if (!url || !url.match(/^https?:\/\/.+$/g)) {
        sendUserError('"' + url + '" is not a valid url.');
    }
    try {
        var content = fetchJSON(url);
    } catch (e) {
        sendUserError(
            'Your request could not be cached. The rows of your dataset probably exceed the 100KB cache limit.'
        );
    }
    if (!content) sendUserError('"' + url + '" returned no content.');

    return content;
}

/**
 * Matches the field value to a semantic
 *
 * @param   {Mixed}   value   The field value
 * @param   {Object}  types   The list of types
 * @return  {string}          The semantic type
 */
function getSemanticType(value, types) {
    if (isNumeric(value)) {
        return types.NUMBER;
    } else if (value === true || value === false) {
        return types.BOOLEAN;
    } else if (!isNaN(Date.parse(value))) {
        return types.YEAR_MONTH_DAY_HOUR;
    } else if (validURL(value)) {
        return types.URL;
    }

    return types.TEXT;
}

/**
 *  Creates the fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {Mixed}   value   The value of the current element
 */
function createField(fields, types, key, value) {
    var aggregations = cc.AggregationType;
    var semanticType = getSemanticType(value, types);
    var field = semanticType === types.NUMBER ? fields.newMetric() : fields.newDimension();

    field.setId(key.replace(/\s/g, '_').toLowerCase());
    field.setName(key);
    field.setDescription(key + ' - ' + types.NUMBER + ' - ' + semanticType);
    field.setType(semanticType);
    if (semanticType === types.NUMBER) {
        field.setAggregation(aggregations.SUM);
    }
}

/**
 * Handles keys for recursive fields
 *
 * @param   {String}  currentKey  The key value of the current element
 * @param   {Mixed}   key         The key value of the parent element
 * @returns {String}  if true
 */
function getElementKey(key, currentKey) {
    if (currentKey === '' || currentKey === null) {
        return '';
    }

    if (key != null) {
        return key + '.' + currentKey.replace('.', '_');
    }

    return currentKey.replace('.', '_');
}

/**
 * Extracts the objects recursive fields and adds it to fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {any[]}   value   The value of the current element
 */
function createFields(fields, types, key, value) {
    if (typeof value === 'object' && value !== null) {
        Object.keys(value).forEach(function (currentKey) {
            var elementKey = getElementKey(key, currentKey);

            if (value[currentKey] !== null && typeof value[currentKey] === 'object') {
                createFields(fields, types, elementKey, value[currentKey]);
            } else {
                createField(fields, types, currentKey, value[currentKey]);
            }
        });
    } else if (key !== null) {
        createField(fields, types, key, value);
    }
}

/**
 * Parses first line of content to determine the data schema
 *
 * @param   {Object}  request getSchema/getData request parameter.
 * @param   {Object}  content The content object
 * @return  {Object}           An object with the connector configuration
 */
function getFields(request, content) {
    var fields = cc.getFields();
    var types = cc.FieldType;
    var aggregations = cc.AggregationType;

    if (!Array.isArray(content)) content = [content];

    if (typeof content[0] !== 'object' || content[0] === null) {
        sendUserError('Invalid JSON format');
    }

    try {
        createFields(fields, types, null, content[0] );
    } catch (e) {
        sendUserError('Unable to identify the data format of one of your fields.');
    }

    return fields;
}

/**
 *  Converts date strings to YYYYMMDDHH:mm:ss
 *
 * @param   {String} val  Date string
 * @returns {String}      Converted date string
 */
function convertDate(val) {
    var date = new Date(val);
    return (
        date.getUTCFullYear() +
        ('0' + (date.getUTCMonth() + 1)).slice(-2) +
        ('0' + date.getUTCDate()).slice(-2) +
        ('0' + date.getUTCHours()).slice(-2)
    );
}

/**
 * Validates the row values. Only numbers, boolean, date and strings are allowed
 *
 * @param   {Field} field The field declaration
 * @param   {Mixed} val   The value to validate
 * @returns {Mixed}       Either a string or number
 */
function validateValue(field, val) {
    if (field.getType() == 'YEAR_MONTH_DAY_HOUR') {
        val = convertDate(val);
    }

    switch (typeof val) {
        case 'string':
        case 'number':
        case 'boolean':
            return val;
        case 'object':
            return JSON.stringify(val);
    }

    return null;
}

/**
 * Returns the (nested) values for requested columns
 *
 * @param   {Object} valuePaths       Field name. If nested; field name and parent field name
 * @param   {Object} row              Current content row
 * @returns {any}                   The field values for the columns
 */
function getColumnValue(valuePaths, row) {
    for (var index in valuePaths) {
        var currentPath = valuePaths[index];

        if (row[currentPath] === null) {
            return '';
        }

        if (row[currentPath] !== undefined) {
            row = row[currentPath];
            continue;
        }
        var keys = Object.keys(row);

        for (var index_keys in keys) {
            var key = keys[index_keys].replace(/\s/g, '_').toLowerCase();
            if (key === currentPath) {
                row = row[keys[index_keys]];
                break;
            }
        }
    }
    return row;
}

/**
 * Returns an object containing only the requested columns
 *
 * @param   {Object} content          The content object
 * @param   {Object} requestedFields  Fields requested in the getData request.
 * @returns {Object}                  An object only containing the requested columns.
 */
function getColumns(content, requestedFields) {
    if (!Array.isArray(content)) content = [content];

    return content.map(function (row) {
        var rowValues = [];

        requestedFields.asArray().forEach(function (field) {
            var valuePaths = field.getId().split('.');
            var fieldValue = row === null ? '' : getColumnValue(valuePaths, row);

            rowValues.push(validateValue(field, fieldValue));
        });
        return {values: rowValues};
    });
}

/**
 * function  `isAdminUser()`
 *
 * @returns {Boolean} Currently just returns false. Should return true if the current authenticated user at the time
 *                    of function execution is an admin user of the connector.
 */
function isAdminUser() {
    return false;
}

/**
 * function  `getAuthType()`
 *
 * @returns {Object} `AuthType` used by the connector.
 */
function getAuthType() {
    return {type: 'NONE'};
}

/**
 * Returns the user configurable options for the connector.
 *
 * Required function for Community Connector.
 *
 * @param   {Object} request  Config request parameters.
 * @returns {Object}          Connector configuration to be displayed to the user.
 */
function getConfig(request) {

    var config = cc.getConfig();

    config
        .newInfo()
        .setId('instructions')
        .setText('Fill out the form to connect to a JSON data source.');

    config
        .newTextInput()
        .setId('url')
        .setName('Enter the URL of a JSON data source')
        .setHelpText('e.g. https://wp-domain-url.org/')
        .setPlaceholder('https://wp-domain-url.org/');

    config.setDateRangeRequired(false);

    return config.build();
}

/**
 * Returns the schema for the given request.
 *
 * @param   {Object} request Schema request parameters.
 * @returns {Object} Schema for the given request.
 */
function getSchema(request) {
    var content = fetchData(request.configParams.url);
    var fields = getFields(request, content).build();
    return {schema: fields};
}

/**
 * Returns the tabular data for the given request.
 *
 * @param   {Object} request  Data request parameters.
 * @returns {Object}          Contains the schema and data for the given request.
 */
function getData(request) {
    var content = fetchData(request.configParams.url);
    var fields = getFields(request, content);
    var requestedFieldIds = request.fields.map(function (field) {
        return field.name;
    });
    var requestedFields = fields.forIds(requestedFieldIds);

    return {
        schema: requestedFields.build(),
        rows: getColumns(content, requestedFields)
    };
}
