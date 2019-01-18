<?php

namespace common\components\queryBuilder;

use Gdbots\QueryParser\Enum\BoolOperator;
use Gdbots\QueryParser\Enum\ComparisonOperator;
use Gdbots\QueryParser\Node\Date;
use Gdbots\QueryParser\Node\DateRange;
use Gdbots\QueryParser\Node\Field;
use Gdbots\QueryParser\Node\Hashtag as HashtagNode;
use Gdbots\QueryParser\Node\Node;
use Gdbots\QueryParser\Node\NumberRange;
use Gdbots\QueryParser\Node\Numbr;
use Gdbots\QueryParser\Node\Phrase;
use Gdbots\QueryParser\Node\Subquery;
use Gdbots\QueryParser\Node\Word;
use Gdbots\QueryParser\QueryParser;
use yii\db\ActiveQuery;
use Yii;
use yii\db\ActiveQueryInterface;
use yii\db\ActiveRecord;
use yii\db\ColumnSchema;
use yii\db\Connection;
use yii\db\Query;
use yii\validators\IpValidator;
use yii\web\Application;
use yii\web\HttpException;

/**
 * Class QueryBuilder
 * @package common\components\queryBuilder
 *
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 */
final class QueryBuilder
{
    const TYPE_BOOLEAN = ['boolean'];
    const TYPE_DATE    = ['timestamp', 'datetime', 'date'];
    const TYPE_NUMERIC = ['integer', 'smallint', 'bigint', 'float', 'decimal', 'money', 'double', 'string', 'text'];
    const TYPE_STRING  = ['string', 'text'];
    const TYPE_INET    = ['inet'];
    const AND_WHERE    = 'AND';
    const OR_WHERE     = 'OR';
    const NOT          = 'NOT';

    /** @var ActiveQuery */
    private $query;
    /** @var QueryParser  */
    private $parser;
    /** @var int */
    private $paramCounter = 0;
    /** @var string|null  */
    private $queryString;
    /** @var Connection */
    private $db;
    /**
     * @var string
     */
    private $paramPrefix;
    /**
     * @var Hashtag[]
     */
    private $hashtags;
    /**
     * @var string
     */
    private $tableName;

    /**
     * QueryBuilder constructor.
     *
     * @param ActiveQuery $query
     * @param string|null $queryString
     * @param array       $hashtags
     */
    public function __construct(ActiveQuery $query, string $queryString = null, array $hashtags = [])
    {
        /** @var Application|\yii\console\Application $app */
        $app = Yii::$app;
        $this->queryString = trim($queryString) === '' ? null : $queryString;
        $this->query   = $query;
        $this->parser  = new LuceneQueryParser();
        $this->db      = $app->getDb();
        $this->paramPrefix = uniqid(':lucene_qb_') . '_';
        $this->hashtags = $hashtags;
        $model = new $this->query->modelClass;
        $this->tableName = $model->tableName();
    }

    /**
     * @return ActiveQuery
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     */
    public function buildQuery()
    {
        if ($this->queryString !== null) {
            $this->processQuery($this->queryString);
        }

        return $this->query;
    }

    /**
     * @param array             $types
     * @param ColumnSchema|null $column
     *
     * @return ColumnSchema[]
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     */
    private function prepareColumns(array $types, ColumnSchema $column = null)
    {
        if ($column === null) {
            /** @var ActiveRecord $model */
            $model = $this->query->modelClass;
            //phpcs:ignore PHPCS_SecurityAudit.BadFunctions.CallbackFunctions.WarnFringestuff
            return array_filter($model::getTableSchema()->columns, function (ColumnSchema $column) use ($types) {
                return in_array($column->type, $types);
            });
        }

        if (in_array($column->type, $types)) {
            return [$column];
        }

        $message = Yii::t('app', 'Wrong attribute type. {attribute} should be {type} but {send} given.', [
            'attribute' => $column->name,
            'type'      => implode(', ', $types),
            'send'      => $column->type,
        ]);
        throw new HttpException(400, $message);
    }

    /**
     * @param string $queryString
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     */
    private function processQuery($queryString)
    {
        $parsedNodes = $this->parser->parse($queryString);
        foreach ($parsedNodes->getNodes() as $node) {
            $this->processNode($node, $node->getBoolOperator(), $this->query, $this->tableName);
        }
    }

    /**
     * @param Node              $node
     * @param BoolOperator      $boolOperator
     * @param ActiveQuery|Query $query
     * @param string|null       $tableName
     * @param ColumnSchema|null $column
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processNode($node, $boolOperator, $query, $tableName = null, $column = null)
    {
        switch (true) {
            case $node instanceof Phrase:
            case $node instanceof Word:
                if ($column !== null && in_array($column->type, self::TYPE_BOOLEAN)) {
                    $this->processBooleanNode($node, $boolOperator, $query, $tableName, $column);
                    break;
                }
                $this->processWordNode($node, $boolOperator, $query, $tableName, $column);
                break;
            case $node instanceof Numbr:
                $this->processNumberNode($node, $boolOperator, $query, $tableName, $column);
                break;
            case $node instanceof Date:
                $this->processDateNode($node, $boolOperator, $query, $tableName, $column);
                break;
            case $node instanceof Field:
                $this->processFieldNode($node);
                break;
            case $node instanceof DateRange:
            case $node instanceof NumberRange:
                $this->processRangeNode($node, $boolOperator, $column, $query, $tableName);
                break;
            case $node instanceof Subquery:
                $this->processSubQueryNode($node, $boolOperator, $column, $query);
                break;
            case $node instanceof HashtagNode:
                $this->handleHashtag($node);
                break;
        }
    }

    /**
     * @param Field $node
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     */
    private function processFieldNode(Field $node)
    {
        /** @var ActiveRecord $model */
        $model      = new $this->query->modelClass;
        $tableName  = null;
        $columns = [];
        foreach ($model::getTableSchema()->columns as $key => $column) {
            $clonedColumn = clone $column;
            $clonedColumn->name = $this->tableName . '.' . $column->name;
            $columns[$key] = $clonedColumn;
        }
        $field      = $node->getValue();

        /** @var ActiveQuery|ActiveQueryInterface|null $relation */
        $relation = $model->getRelation($field, false);
        if (strpos($field, '.') !== false || $relation !== null) {
            $this->processRelation($field, $node);
            return;
        }

        if (!isset($columns[$field])) {
            $message = Yii::t('app', 'Attribute \'{attribute}\' not found in model {model}', [
                'attribute' => $field,
                'model'     => $this->tableName,
            ]);
            throw new HttpException(400, $message);
        }

        /** @var Node $subNode */
        $subNode = $this->processModelMapping($field, $node->getNode());

        $this->processNode($subNode, $node->getBoolOperator(), $this->query, $tableName, $columns[$field]);
    }

    /**
     * @param Subquery          $subNode
     * @param ColumnSchema|null $column
     * @param BoolOperator      $boolOperator
     * @param ActiveQuery|Query $query
     *
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processSubQueryNode($subNode, BoolOperator $boolOperator, $column, $query)
    {
        if ($column === null) {
            return;
        }
        //phpcs:ignore PHPCS_SecurityAudit.BadFunctions.CallbackFunctions.WarnFringestuff
        $values = array_map(function (Node $node) {
            return $node->getValue();
        }, $subNode->getNodes());

        $modelClass = new $this->query->modelClass;

        foreach ($values as $key => $value) {
            if ($modelClass instanceof AttributeMappingInterface && $modelClass->attributeMap($column->name) !== []) {
                $values[$key] = $modelClass->getMappingByAttribute($column->name, $value);
            }
        }

        $operatorValue = $boolOperator->getValue();
        $op = $operatorValue === BoolOperator::PROHIBITED ? 'NOT IN' : 'IN';
        $method = $operatorValue === BoolOperator::OPTIONAL ? 'orWhere' : 'andWhere';
        if (in_array($column->type, self::TYPE_STRING)) {
            $op = $operatorValue === BoolOperator::PROHIBITED ? '!~*' : '~*';
            $values = '(' . implode('|', $values) . ')';
        }

        $query->$method([$op, $column->name, $values]);
    }

    /**
     * @param Date              $node
     * @param BoolOperator      $boolOperator
     * @param ActiveQuery|Query $query
     * @param string|null       $tableName
     * @param ColumnSchema|null $column
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processDateNode(Date $node, BoolOperator $boolOperator, $query, $tableName = null, ColumnSchema $column = null)
    {
        $operatorValue = $boolOperator->getValue();

        $conditions = [$operatorValue != BoolOperator::PROHIBITED ? 'OR' : 'AND'];
        $params     = [];

        foreach ($this->prepareColumns(self::TYPE_DATE, $column) as $column) {
            $comparison    = $this->resolveComparison($node->getComparisonOperator(), $boolOperator, $column->type);
            $dateTime = $node->toDateTime()->format('H:i') === '00:00' ? $node->toDateTime()->format('Y-m-d') : $node->toDateTime()->format(\DateTime::ATOM);
            $value         = in_array($column->type, self::TYPE_STRING) ? ('%' . $node->getValue() . '%') : $dateTime;
            $operatorValue = $boolOperator->getValue();
            $param         = $this->paramPrefix . $this->paramCounter++;
            $params[$param] = $value;

            $colName = $tableName === null ? $column->name : "$tableName.{$column->name}";

            if ($operatorValue != BoolOperator::PROHIBITED) {
                $conditions[] = "$colName $comparison $param";
                continue;
            }
            $conditions[] = "$colName IS NULL OR $colName $comparison $param";
        }

        switch ($operatorValue) {
            case BoolOperator::OPTIONAL:
                $query->orWhere($conditions, $params);
                break;
            default:
            case BoolOperator::REQUIRED:
                $query->andWhere($conditions, $params);
                break;
            case BoolOperator::PROHIBITED:
                $query->andWhere($conditions, $params);
                break;
        }
    }

    /**
     * @param Numbr             $node
     * @param BoolOperator      $boolOperator
     * @param ActiveQuery|Query $query
     * @param string|null       $tableName
     * @param ColumnSchema|null $column
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processNumberNode(Numbr $node, BoolOperator $boolOperator, $query, $tableName = null, $column = null)
    {
        $operatorValue = $boolOperator->getValue();

        $conditions = [$operatorValue != BoolOperator::PROHIBITED ? 'OR' : 'AND'];
        $params     = [];

        foreach ($this->prepareColumns(self::TYPE_NUMERIC, $column) as $column) {
            if (in_array($column->dbType, self::TYPE_INET)) {
                continue;
            }
            $comparison = $this->resolveComparison($node->getComparisonOperator(), $boolOperator, $column->type);
            $value      = in_array($column->type, self::TYPE_STRING) ? ('%' . $node->getValue() . '%') : $node->getValue();
            $param = $this->paramPrefix . $this->paramCounter;
            $this->paramCounter++;
            $params[$param] = $value;
            $colName        = $tableName === null ? $column->name : "$tableName.{$column->name}";

            if ($operatorValue != BoolOperator::PROHIBITED) {
                $conditions[] = "$colName $comparison $param";
                continue;
            }
            $conditions[] = "$colName IS NULL OR $colName $comparison $param";
        }

        switch ($operatorValue) {
            case BoolOperator::OPTIONAL:
                $query->orWhere($conditions, $params);
                break;
            default:
            case BoolOperator::REQUIRED:
                $query->andWhere($conditions, $params);
                break;
            case BoolOperator::PROHIBITED:
                $query->andWhere($conditions, $params);
                break;
        }
    }

    /**
     * @param Word|Phrase       $node
     * @param BoolOperator      $boolOperator
     * @param ActiveQuery|Query $query
     * @param string|null       $tableName
     *
     * @param ColumnSchema|null $column
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processBooleanNode($node, BoolOperator $boolOperator, $query, $tableName = null, $column = null)
    {
        $operatorValue = $boolOperator->getValue();

        $conditions = [$operatorValue != BoolOperator::PROHIBITED ? 'OR' : 'AND'];
        $param      = $this->paramPrefix . $this->paramCounter++;
        $params     = [$param => $node->getValue() === 'true' ? true : false];

        foreach ($this->prepareColumns(self::TYPE_BOOLEAN, $column) as $column) {
            $colName = $tableName === null ? $column->name : "$tableName.{$column->name}";
            if ($operatorValue != BoolOperator::PROHIBITED) {
                $conditions[] = "$colName = $param";
                continue;
            }
            $conditions[] = "$colName IS NULL OR $colName != $param";
        }

        switch ($operatorValue) {
            case BoolOperator::OPTIONAL:
                $query->orWhere($conditions, $params);
                break;
            default:
            case BoolOperator::REQUIRED:
                $query->andWhere($conditions, $params);
                break;
            case BoolOperator::PROHIBITED:
                $query->andWhere($conditions, $params);
                break;
        }
    }

    /**
     * @param Word|Phrase       $node
     * @param BoolOperator      $boolOperator
     * @param ActiveQuery|Query $query
     * @param string|null       $tableName
     *
     * @param ColumnSchema|null $column
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processWordNode($node, BoolOperator $boolOperator, $query, $tableName = null, $column = null)
    {
        $operatorValue = $boolOperator->getValue();

        $conditions = [$operatorValue != BoolOperator::PROHIBITED ? 'OR' : 'AND'];
        $param = $this->paramPrefix . $this->paramCounter++;
        $nodeValue = $node->getValue();
        $params = [$param => '%' . $nodeValue . '%'];

        foreach ($this->prepareColumns(self::TYPE_STRING, $column) as $column) {
            $colName = $tableName === null ? $column->name : "$tableName.{$column->name}";
            if (in_array($column->dbType, self::TYPE_INET)) {
                $validator = new IpValidator();
                if ($validator->validate($nodeValue)) {
                    $inetParam = $this->paramPrefix . $this->paramCounter++;
                    $params[$inetParam] = $nodeValue;
                    $conditions[] = "$colName = $inetParam";
                }
                continue;
            }
            if ($operatorValue != BoolOperator::PROHIBITED) {
                $conditions[] = "$colName ILIKE $param";
                continue;
            }
            $conditions[] = "$colName IS NULL OR $colName NOT ILIKE $param";
        }

        switch ($operatorValue) {
            case BoolOperator::OPTIONAL:
                $query->orWhere($conditions, $params);
                break;
            default:
            case BoolOperator::REQUIRED:
                $query->andWhere($conditions, $params);
                break;
            case BoolOperator::PROHIBITED:
                $query->andWhere($conditions, $params);
                break;
        }
    }

    /**
     * @param NumberRange|DateRange $node
     * @param BoolOperator          $boolOperator
     * @param ColumnSchema|null     $column
     * @param ActiveQuery|Query     $query
     * @param string|null           $tableName
     *
     * @throws HttpException
     * @SuppressWarnings(PHPMD.BooleanArgumentFlag)
     */
    private function processRangeNode($node, BoolOperator $boolOperator, $column, $query, $tableName = null)
    {
        if ($column === null) {
            return;
        }
        $lowerNode = $node->getLowerNode();
        $upperNode = $node->getUpperNode();

        $nodeType = $node instanceof DateRange ? self::TYPE_DATE : self::TYPE_NUMERIC;

        if (!in_array($column->type, $nodeType)) {
            $message = Yii::t('app', 'Wrong attribute type. {attribute} should be {expected} but {send} given.', [
                'attribute' => $column->name,
                'send'      => $column->type,
                'expected'  => implode(', ', $nodeType),
            ]);
            throw new HttpException(400, $message);
        }

        $operatorValue = $boolOperator->getValue();

        $operator = $node->isExclusive() ? '' : '=';

        $where = ['AND'];
        $params = [];
        $columnName = $tableName === null ? $column->name : $tableName . '.' . $column->name;
        if ($lowerNode !== null) {
            $param = $this->paramPrefix . $this->paramCounter++;
            $where[] = "{$columnName} >{$operator} {$param}";
            $params[$param] = $lowerNode->getValue();
        }

        if ($upperNode !== null) {
            $param = $this->paramPrefix . $this->paramCounter++;
            $where[] = "{$columnName} <{$operator} {$param}";
            $params[$param] = $upperNode->getValue();
        }

        switch ($operatorValue) {
            case BoolOperator::OPTIONAL:
                $query->orWhere($where, $params);
                break;
            case BoolOperator::REQUIRED:
                $query->andWhere($where, $params);
                break;
            case BoolOperator::PROHIBITED:
                $query->andWhere(['not', $where], $params);
                break;
        }
    }

    /**
     * @param string $field
     * @param Field  $node
     *
     * @throws HttpException
     * @throws \yii\base\InvalidConfigException
     */
    private function processRelation($field, $node)
    {
        $data = explode('.', $field, 2);
        $relationName = $data[0];
        /** @var ActiveRecord $model */
        $model         = new $this->query->modelClass;
        /** @var ActiveQuery|null $relation */
        $relation      = $model->getRelation($relationName, false);
        if ($relation === null) {
            $message = Yii::t('app', '{model} has no relation named {relation}', [
                'model'    => get_class($model),
                'relation' => $relationName,
            ]);
            throw new HttpException(400, $message);
        }

        /** @var ActiveRecord $relationModel */
        $relationModel = new $relation->modelClass;
        $relationTable = $relationModel->tableName();
        //phpcs:ignore PHPCS_SecurityAudit.BadFunctions.FilesystemFunctions.WarnWeirdFilesystem
        $relationLink  = $relation->link;

        $primaryKey = $relationModel::getTableSchema()->primaryKey;
        if (count($primaryKey) !== 1 && count($data) !== 2) {
            $message = Yii::t('app', 'Related model {model} must define attribute to search for or have one column primary key', [
                'model'    => get_class($relationModel),
            ]);
            throw new HttpException(400, $message);
        }

        $relationField = count($data) === 2 ? $data[1] : reset($primaryKey);
        /** @var Node $subNode */
        $columns = $relationModel::getTableSchema()->columns;
        $column  = $columns[$relationField];

        $subQuery = new Query();
        $subQuery->select(key($relationLink))->from($relationTable);

        if ($relation->via === null) {
            $subNode = $this->processModelMapping($field, $node->getNode());
            $this->processNode($subNode, $node->getBoolOperator(), $subQuery, $relationTable, $column);
            $this->query->andWhere(['in', reset($relationLink), $subQuery]);
            return;
        }

        $viaSubQuery = new Query();
        $viaSubQuery->select(key($relationLink))->from($relationTable);

        $subNode = $this->processModelMapping($field, $node->getNode());
        $this->processNode($subNode, $node->getBoolOperator(), $viaSubQuery, $relationTable, $column);

        $relationTable = $relation->via->from[0];
        //phpcs:ignore PHPCS_SecurityAudit.BadFunctions.FilesystemFunctions.WarnWeirdFilesystem
        $relationViaLink = $relation->via->link;
        $subQuery->select(key($relationViaLink))->from($relationTable);
        $subQuery->andWhere(['in', reset($relationLink), $viaSubQuery]);
        $this->query->andWhere(['in', reset($relationViaLink), $subQuery]);
    }

    /**
     * @param string $field
     * @param Node   $subNode
     *
     * @return Node
     */
    private function processModelMapping($field, $subNode)
    {
        $modelClass = new $this->query->modelClass;

        if (!$modelClass instanceof AttributeMappingInterface || $subNode instanceof Subquery || $modelClass->attributeMap($field) === []) {
            return $subNode;
        }

        $value = $modelClass->getMappingByAttribute($field, $subNode->getValue());
        $type  = gettype($value);

        if (in_array($type, self::TYPE_STRING)) {
            $subNode = new Word($value, $subNode->getBoolOperator());
        }
        if (in_array($type, self::TYPE_NUMERIC)) {
            $subNode = new Numbr($value);
        }
        if (in_array($type, self::TYPE_DATE)) {
            $subNode = new Date($value, $subNode->getBoolOperator());
        }

        return $subNode;
    }

    /**
     * @param ComparisonOperator $operator
     * @param BoolOperator       $boolOperator
     * @param string             $columnType
     *
     * @return string
     */
    private function resolveComparison(ComparisonOperator $operator, BoolOperator $boolOperator, $columnType)
    {
        $operator = $operator->getValue();
        if ($boolOperator->getValue() === BoolOperator::PROHIBITED) {
            return in_array($columnType, self::TYPE_STRING) ? 'NOT ILIKE' : '!=';
        }

        switch ($operator) {
            default:
            case ComparisonOperator::EQ:
                return in_array($columnType, self::TYPE_STRING) ? 'ILIKE' : '=';
            case ComparisonOperator::GT:
                return '>';
            case ComparisonOperator::GTE:
                return '>=';
            case ComparisonOperator::LT:
                return '<';
            case ComparisonOperator::LTE:
                return '<=';
        }
    }

    private function handleHashtag(HashtagNode $node)
    {
        $callable = $this->hashtags[$node->getValue()] ?? null;
        if (is_callable($callable)) {
            $callable($this->query);
            return;
        }

        if ($callable !== null) {
            throw new HttpException(400, "Hashtag '#{$node->getValue()}' must be callable");
        }

        $reflection = new \ReflectionClass($this->query);
        if (!$reflection->hasMethod($node->getValue())) {
            throw new HttpException(400, "Hashtag '#{$node->getValue()}' is not defined");
        }

        $method = $reflection->getMethod($node->getValue());
        if (!$method->isPublic() || $method->getNumberOfParameters() !== 0) {
            throw new HttpException(400, "Hashtag '#{$node->getValue()}' is found but is not public or requires arguments");
        }

        $this->query->{$node->getValue()}();
    }
}
