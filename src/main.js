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
 * @param   {string}  V2ObjectType
 *
 * @return  {string}          The semantic type
 */
function getSemanticType(value, types, V2ObjectType) {
    if (V2ObjectType) {
        switch (V2ObjectType) {
            case 'NUMBER':
                return types.NUMBER;

            case 'PERCENT':
                return types.PERCENT;

            case 'TEXT':
                return types.TEXT;

            case 'BOOLEAN':
                return types.BOOLEAN;

            case 'URL':
                return types.URL;

            case 'HYPERLINK':
                return types.URL;

            case 'IMAGE':
                return types.IMAGE;

            case 'IMAGELINK':
                return types.IMAGELINK;

            case 'YEAR':
                return types.YEAR;

            case 'YEAR_QUARTER':
                return types.YEAR_QUARTER;

            case 'YEAR_MONTH':
                return types.YEAR_MONTH;

            case 'YEAR_WEEK':
                return types.YEAR_WEEK;

            case 'YEAR_MONTH_DAY':
                return types.YEAR_MONTH_DAY;

            case 'YEAR_MONTH_DAY_HOUR':
                return types.YEAR_MONTH_DAY_HOUR;

            case 'YEAR_MONTH_DAY_SECOND':
                return types.YEAR_MONTH_DAY_HOUR;

            case 'QUARTER':
                return types.QUARTER;

            case 'MONTH':
                return types.MONTH;

            case 'WEEK':
                return types.WEEK;

            case 'MONTH_DAY':
                return types.MONTH_DAY;

            case 'DAY_OF_WEEK':
                return types.DAY_OF_WEEK;

            case 'DAY':
                return types.DAY;

            case 'HOUR':
                return types.HOUR;

            case 'MINUTE':
                return types.MINUTE;

            case 'DURATION':
                return types.DURATION;

            case 'COUNTRY':
                return types.COUNTRY;

            case 'COUNTRY_CODE':
                return types.COUNTRY_CODE;

            case 'CONTINENT':
                return types.CONTINENT;

            case 'CONTINENT_CODE':
                return types.CONTINENT_CODE;

            case 'SUB_CONTINENT':
                return types.SUB_CONTINENT;

            case 'SUB_CONTINENT_CODE':
                return types.SUB_CONTINENT_CODE;

            case 'REGION':
                return types.REGION;

            case 'REGION_CODE':
                return types.REGION_CODE;

            case 'CITY':
                return types.CITY;

            case 'CITY_CODE':
                return types.CITY_CODE;

            case 'METRO_CODE':
                return types.METRO_CODE;

            case 'LATITUDE_LONGITUDE':
                return types.LATITUDE_LONGITUDE;

            default:
                return types.TEXT;
        }
    }

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

function getAggregation(V2ObjectAggregation, aggregations) {
    if ('AUTO' === V2ObjectAggregation) {
        return aggregations.AUTO
    }

    if ('SUM' === V2ObjectAggregation) {
        return aggregations.SUM
    }

    if ('MIN' === V2ObjectAggregation) {
        return aggregations.MIN
    }

    if ('MAX' === V2ObjectAggregation) {
        return aggregations.MAX
    }

    if ('COUNT' === V2ObjectAggregation) {
        return aggregations.COUNT
    }

    if ('COUNT_DISTINCT' === V2ObjectAggregation) {
        return aggregations.COUNT_DISTINCT
    }

    if ('AVG' === V2ObjectAggregation) {
        return aggregations.AVG
    }

    return aggregations.AUT;
}

/**
 *  Creates the fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {Mixed}   value   The value of the current element
 * @param   {Object}   V2Object   The value of the current element
 */
function createField(fields, types, key, value, V2Object) {
    var V2ObjectType = V2Object && V2Object.type ? V2Object.type : ''
    var aggregations = cc.AggregationType;
    var semanticType = getSemanticType(value, types, V2ObjectType);
    var field = semanticType === types.NUMBER ? fields.newMetric() : fields.newDimension();

    var fieldKey = key.replace(/\s/g, '_').toLowerCase();
    var fieldName = V2Object ? V2Object.name : key;
    var fieldDescription = V2Object ? V2Object.description : key;
    var fieldAggregation = V2Object ? getAggregation(V2Object.aggregation, aggregations) : aggregations.SUM;

    field.setId(fieldKey);
    field.setName(fieldName);
    field.setDescription(fieldDescription);
    field.setType(semanticType);
    if (semanticType === types.NUMBER) {
        if (V2Object && V2Object.aggregation !== 'NONE') {
            field.setAggregation(fieldAggregation);
        } else {
            field.setAggregation(aggregations.SUM);
        }
    }

    // if (V2Object && V2Object.formula !== '') {
    //     field.setFormula(V2Object.formula);
    // }
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

    if (!Array.isArray(content)) content = [content];

    if (typeof content[0] !== 'object' || content[0] === null) {
        sendUserError('Invalid JSON format');
    }

    try {
        createFields(fields, types, null, content[0]);
    } catch (e) {
        sendUserError('Unable to identify the data format of one of your fields.');
    }

    return fields;
}

/**
 * Parse fields for v2.
 *
 * @param   {Object}  request getSchema/getData request parameter.
 * @param   {Object}  content The content object
 * @return  {Object}           An object with the connector configuration
 */
function getFieldsV2(request, content) {
    var fields = cc.getFields();
    var types = cc.FieldType;
    if (typeof content !== 'object' || content === null) {
        sendUserError('Invalid JSON format');
    }

    try {
        Object.keys(content).forEach(function (currentKey) {
            createField(fields, types, currentKey, content[currentKey].value, content[currentKey]);
        });
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

    config
        .newTextInput()
        .setId('subscription_key')
        .setName('Enter Subscription Key')
        .setHelpText('Free to use any random string for now ');

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
    var url_args = request.configParams.subscription_key ? '?skeleton=1&skeleton_type=1' : '';
    var content = fetchData(request.configParams.url + url_args);
    var fields = '';
    if (request.configParams.subscription_key) {
        fields = getFieldsV2(request, content).build();
    } else {
        fields = getFields(request, content).build();
    }

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
