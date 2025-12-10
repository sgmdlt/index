import { JSONPathJS } from 'jsonpath-js';
import { parse, isValid } from 'date-fns';
import { format as formatDate } from 'date-fns';
import rules from './elastic_index_dict.json' with { type: "json" };
import typeInstanceRules from './elastic_type_instance_dict.json' with { type: "json" };

const jsonPathCache = new Map();

function getCompiledJSONPath(path) {
  let query = jsonPathCache.get(path);
  if (!query) {
    query = new JSONPathJS(path);
    jsonPathCache.set(path, query);
  }
  return query;
};

function getValueByPath(json, path) {
  const query = getCompiledJSONPath(path);
  return query.find(json);
};

function getStringValue(values) {
  // find first non-null value
  const value = values.find(v => v !== null && v !== undefined);
  if (value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    return value.trim();
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value).trim();
  }
  return '';
}

function getMergeListValue(values) {
  return values.filter(v => v)
    .map(v => {
      if (Array.isArray(v)) {
        return v.map(item => {
          if (typeof item === 'object') {
            return item.name;
          }
          return item;
        }).filter(item => item).join('; ');
      }
      return v;
    })
    .join('\n');
}

function getStringListValue(values) {
  return values
    .filter(d => d && d.length)
    .map(d => d.trim());
}

function objectToString(obj) {
  return Object.values(obj).filter(v => v).join(' - ');
}

function getDateValue(values, format) {
  if (!values[0]) {
    return null;
  }
  if (format !== 'yyyy-MM-dd') {
    const date = parse(values[0], format, new Date());
    if (!isValid(date)) {
      return null;
    }
    return formatDate(date, 'yyyy-MM-dd');
  }
  return values[0];
}

function getBooleanValue(values) {
  return values[0] === true;
}

const typeMapping = {
  string: getStringValue,
  merge: getMergeListValue,
  date: getDateValue,
  boolean: getBooleanValue,
  list: getStringListValue,
}

function isEmptyValue(value) {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return true;
    }
    if (value.map(v => v && v.trim ? v.trim() : v).every(v => v === '')) {
      return true;
    }
    if (!value.find(v => v)) {
      return true;
    }
  }
  if (value === undefined || value === null) {
    return true;
  }
  return typeof value === 'string' && value.trim() === '';
}

function getValue(json, locations, type) {
  let locationsWithValues = [];
  for (let location of locations) {
    let format;
    let path = location;
    if (typeof location === 'object') {
      format = location.format;
      path = location.key;
    }

    const value = getValueByPath(json, path);
    if (!isEmptyValue(value)) {
      locationsWithValues.push([location, value]);
      const result = typeMapping[type](value, format)
      if (result) {
        return result;
      }
    }
  }
  if (locationsWithValues.length) {
    console.log('All locations processed but no valid value found for', locationsWithValues, 'in json', json);
  }
}

function getTypeInstance(typeInstance) {
  if (typeInstanceRules[typeInstance]) {
    return typeInstanceRules[typeInstance];
  }
  return null;
}

const PARTICIPANTS_AND_DEFENDANTS = 'participants_and_defendants';
const TYPE_INSTANCE = 'type_instance';
const CASE_TYPE = 'case_type';
const INSTANCE = 'instance';

export default async function prepareDoc(raw) {
  const doc = {};
  for (const [key, config] of Object.entries(rules)) {
    const type = config.type || 'string';
    try {
      const value = getValue(raw, config.locations, type);
      if (value) {
        doc[key] = value;
      }
    } catch (e) {
      console.log('Error processing', raw, key, config, type);
      throw e;
    }
  }

  // add additional fields

  // participants_and_defendants
  doc[PARTICIPANTS_AND_DEFENDANTS] = [doc['defendants'], doc['participants']]
    .filter(v => v)
    .join('\n');

  // case_type and instance are overwritten by type_instance if present
  if (raw[TYPE_INSTANCE]) {
    const typeInstance = getTypeInstance(raw[TYPE_INSTANCE]);
    if (typeInstance) {
      doc[CASE_TYPE] = typeInstance[CASE_TYPE];
      doc[INSTANCE] = typeInstance[INSTANCE];
    }
  }

  // add text length
  if (doc.main_document_text) {
    doc.main_document_text_length = doc.main_document_text.length;
  }

  return doc;
}
